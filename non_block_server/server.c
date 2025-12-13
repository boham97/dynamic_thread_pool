#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <libpq-fe.h>

#define PORT 8080
#define MAX_EVENTS 10
#define BUF_SIZE 1024
#define POOL_SIZE 10
#define QUEUE_SIZE 1000

// PostgreSQL 연결 정보
#define DB_HOST "172.17.0.3"
#define DB_PORT "5432"
#define DB_NAME "pgdb"
#define DB_USER "pguser"
#define DB_PASS "pgpass"

typedef struct {
    int fd;
    char buf[BUF_SIZE];
    size_t buf_len;
} sock_buffer_t;

typedef struct {
    char data[BUF_SIZE];
    int len;
    int client_fd;
} db_task_t;

typedef struct {
    PGconn *conn;
    int in_use;
} pg_conn_t;

typedef struct {
    pg_conn_t pool[POOL_SIZE];
    pthread_mutex_t pool_lock;  // 전체 풀만 보호하면 충분
} pg_pool_t;

typedef struct {
    db_task_t tasks[QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int shutdown;
} task_queue_t;

pg_pool_t *g_pool = NULL;
task_queue_t *g_queue = NULL;

// PostgreSQL 연결 풀 초기화
pg_pool_t* init_pg_pool() {
    pg_pool_t *pool = malloc(sizeof(pg_pool_t));
    pthread_mutex_init(&pool->pool_lock, NULL);
    
    for (int i = 0; i < POOL_SIZE; i++) {
        char conninfo[512];
        snprintf(conninfo, sizeof(conninfo),
                "host=%s port=%s dbname=%s user=%s password=%s",
                DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS);
        
        pool->pool[i].conn = PQconnectdb(conninfo);
        
        if (PQstatus(pool->pool[i].conn) != CONNECTION_OK) {
            fprintf(stderr, "Connection to database failed: %s\n",
                    PQerrorMessage(pool->pool[i].conn));
            PQfinish(pool->pool[i].conn);
            pool->pool[i].conn = NULL;
        }
        
        pool->pool[i].in_use = 0;
    }
    
    return pool;
}

// 연결 풀에서 사용 가능한 연결 가져오기
PGconn* get_pg_conn(pg_pool_t *pool) {
    while (1) {
        pthread_mutex_lock(&pool->pool_lock);
        
        for (int i = 0; i < POOL_SIZE; i++) {
            if (!pool->pool[i].in_use && pool->pool[i].conn != NULL) {
                pool->pool[i].in_use = 1;
                PGconn *conn = pool->pool[i].conn;
                pthread_mutex_unlock(&pool->pool_lock);
                return conn;
            }
        }
        
        pthread_mutex_unlock(&pool->pool_lock);
        usleep(10000); // 10ms 대기
    }
}

// 연결 반환
void release_pg_conn(pg_pool_t *pool, PGconn *conn) {
    pthread_mutex_lock(&pool->pool_lock);
    
    for (int i = 0; i < POOL_SIZE; i++) {
        if (pool->pool[i].conn == conn) {
            pool->pool[i].in_use = 0;
            break;
        }
    }
    
    pthread_mutex_unlock(&pool->pool_lock);
}

// 작업 큐 초기화
task_queue_t* init_task_queue() {
    task_queue_t *queue = malloc(sizeof(task_queue_t));
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    queue->shutdown = 0;
    pthread_mutex_init(&queue->lock, NULL);
    pthread_cond_init(&queue->cond, NULL);
    return queue;
}

// 작업 추가
int enqueue_task(task_queue_t *queue, const char *data, int len, int client_fd) {
    pthread_mutex_lock(&queue->lock);
    
    if (queue->count >= QUEUE_SIZE) {
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }
    
    memcpy(queue->tasks[queue->tail].data, data, len);
    queue->tasks[queue->tail].len = len;
    queue->tasks[queue->tail].client_fd = client_fd;
    
    queue->tail = (queue->tail + 1) % QUEUE_SIZE;
    queue->count++;
    
    pthread_cond_signal(&queue->cond);
    pthread_mutex_unlock(&queue->lock);
    
    return 0;
}

// 작업 가져오기
int dequeue_task(task_queue_t *queue, db_task_t *task) {
    pthread_mutex_lock(&queue->lock);
    
    while (queue->count == 0 && !queue->shutdown) {
        pthread_cond_wait(&queue->cond, &queue->lock);
    }
    
    if (queue->shutdown && queue->count == 0) {
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }
    
    *task = queue->tasks[queue->head];
    queue->head = (queue->head + 1) % QUEUE_SIZE;
    queue->count--;
    
    pthread_mutex_unlock(&queue->lock);
    return 0;
}

// 워커 스레드 - DB에 비동기 저장
void* db_worker(void *arg) {
    while (1) {
        db_task_t task;
        
        if (dequeue_task(g_queue, &task) < 0) {
            break; // shutdown
        }
        
        PGconn *conn = get_pg_conn(g_pool);
        
        // SQL 준비 (테이블: messages, 컬럼: client_fd, data, timestamp)
        const char *paramValues[2];
        char fd_str[32];
        snprintf(fd_str, sizeof(fd_str), "%d", task.client_fd);
        paramValues[0] = fd_str;
        paramValues[1] = task.data;
        
        PGresult *res = PQexecParams(conn,
            "INSERT INTO messages (client_fd, data, timestamp) VALUES ($1, $2, NOW())",
            2,
            NULL,
            paramValues,
            NULL,
            NULL,
            0);
        
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "INSERT failed: %s", PQerrorMessage(conn));
        } else {
            printf("Data saved to DB: fd=%d, len=%d\n", task.client_fd, task.len);
        }
        
        PQclear(res);
        release_pg_conn(g_pool, conn);
    }
    
    return NULL;
}

int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int main() {
    //db pool
    pthread_t workers[4];
    {

        // PostgreSQL 연결 풀 초기화
        g_pool = init_pg_pool();
        printf("PostgreSQL connection pool initialized (%d connections)\n", POOL_SIZE);
        
        // 작업 큐 초기화
        g_queue = init_task_queue();
        
        // 워커 스레드 생성
        for (int i = 0; i < 4; i++) {
            pthread_create(&workers[i], NULL, db_worker, NULL);
        }
        printf("DB worker threads started (4 threads)\n");
        
        // 테이블 생성 (없으면)
        PGconn *conn = get_pg_conn(g_pool);
        PGresult *res = PQexec(conn,
            "CREATE TABLE IF NOT EXISTS messages ("
            "id SERIAL PRIMARY KEY, "
            "client_fd INT, "
            "data TEXT, "
            "timestamp TIMESTAMP)");
            
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            fprintf(stderr, "CREATE TABLE failed: %s", PQerrorMessage(conn));
        }
        PQclear(res);
        release_pg_conn(g_pool, conn);
    }
    
    // 서버 소켓 설정
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    set_nonblock(server_fd);
    
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 128);
    
    printf("Server listening on port %d\n", PORT);

    int epfd = epoll_create1(0);
    struct epoll_event ev, events[MAX_EVENTS];

    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    while (1) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);

        for (int i = 0; i < n; i++) {
            if (events[i].data.fd == server_fd) {
                int client_fd = accept(server_fd, NULL, NULL);
                set_nonblock(client_fd);

                sock_buffer_t *buf = malloc(sizeof(sock_buffer_t));
                buf->fd = client_fd;
                buf->buf_len = 0;

                ev.events = EPOLLIN | EPOLLET;
                ev.data.ptr = buf;
                epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &ev);

                printf("Client connected: fd=%d\n", client_fd);
            } else {
                sock_buffer_t *buf = (sock_buffer_t *)events[i].data.ptr;
                int fd = buf->fd;

                while (1) {
                    ssize_t r = recv(fd, buf->buf, BUF_SIZE, 0);
                    if (r > 0) {
                        buf->buf_len = r;
                        buf->buf[r] = '\0'; // null-terminate
                        printf("recv(fd=%d): %.*s\n", fd, (int)r, buf->buf);
                        
                        // DB에 비동기 저장
                        if (enqueue_task(g_queue, buf->buf, r, fd) < 0) {
                            fprintf(stderr, "Task queue full!\n");
                        }
                    } else if (r == 0) {
                        printf("Client disconnected: fd=%d\n", fd);
                        close(fd);
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                        free(buf);
                        break;
                    } else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break;
                        } else {
                            perror("recv");
                            close(fd);
                            epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                            free(buf);
                            break;
                        }
                    }
                }
            }
        }
    }

    // 종료 처리
    pthread_mutex_lock(&g_queue->lock);
    g_queue->shutdown = 1;
    pthread_cond_broadcast(&g_queue->cond);
    pthread_mutex_unlock(&g_queue->lock);
    
    for (int i = 0; i < 4; i++) {
        pthread_join(workers[i], NULL);
    }

    close(server_fd);
    return 0;
}