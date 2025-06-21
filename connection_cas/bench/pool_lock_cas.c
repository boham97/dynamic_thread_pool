#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>

#define THREAD_CNT 30
#define POOL_MIN_CNT 60
#define POOL_MAX_CNT 60
#define TRY 1000
#define SLEEP 0


//점유 플래그
enum conn_flag
{
    IN_USING = 0,
    //CHECKING, 하우스 키퍼는 한개이므로 
    CLOSED
};


pthread_key_t key;
struct aligned_int 
{
    int value;
    long updated;                   //커넥션 아이들 시작 시간 -> 오래 돠면 제거
    long created;                   //커넥션 생성 시간 -> 오래되면 재 연결
    enum conn_flag flag;            //점유 상황
    char padding[40];               // 64바이트 정렬
} __attribute__((aligned(64)));

int add_conn();
int get_conn();
int get_conn_long();
int get_conn_short();
int is_valid(int i);

void return_conn(int index);
void return_conn_cas(int index);

pthread_mutex_t lock;               // 전역 뮤텍스

int conn_lock_status[POOL_MAX_CNT] = {0,};
struct aligned_int conn_cas_status[POOL_MAX_CNT];
long conn_add_time[POOL_MAX_CNT] = {0,};

volatile long now = 0;

void *house_keeper(void *arg)
{
    int i = 0;
    int conn_count = 0;
    while(1)
    {
        conn_count = 0;
        now = time(NULL);
        for (int i = 0; i < POOL_MAX_CNT; i++) 
        {
            //1.아이들
            if(__sync_bool_compare_and_swap(&conn_cas_status[i].value, 0, 1))
            {
                if(conn_count > POOL_MIN_CNT && conn_cas_status[i].updated + 10 * 60 < now)                 //아이들이 오래 됬다면 제거
                {
                    //대충 연결 해제
                    printf("hk conn close\n");
                    conn_cas_status[i].flag = CLOSED;
                    conn_count--;
                }
                else
                {
                    conn_count++;
                }
                __sync_bool_compare_and_swap(&conn_cas_status[i].value, 1, 0);
            }
            //2. 닫힌 커넥션
            else if (__sync_bool_compare_and_swap(&conn_cas_status[i].value, 1, 1) && conn_cas_status[i].flag == CLOSED)
            {
                //재연결
                //printf("hk reconnect\n");
                conn_cas_status[i].created = now;
                conn_cas_status[i].updated = now;
                conn_cas_status[i].flag = IN_USING;
                __sync_bool_compare_and_swap(&conn_cas_status[i].value, 1, 0);
            }
            else
            {
                //printf("connect work\n ");
                conn_count++;
            }
            //printf("hk %ld %ld\n", conn_cas_status[i].created, conn_cas_status[i].updated);
        }
        //printf("conn cnt: %d\n\n", conn_count);
        sleep(1);
    }
    return NULL;
}

void *worker_lock(void *arg) 
{
    int get = 0;
    int res = 0;
    for (int i = 0; i < TRY; i++) 
    { 
        while(1)
        {
            get++;
            res = get_conn();
            if (res != -1)
            {
                //usleep(SLEEP);
                return_conn(res);
                break;
            }
        }

    }
    printf("%d\n", get);
    return NULL;
}

void *worker_cas(void *arg) 
{
    int res = 0;
    int get = 0;
    int cash = 0;
    for (int i = 0; i < TRY; i++) 
    {
        while(1)
        {
            get++;
            if((res = get_conn_short()) != -1)
            {
                cash++;
                break;
            }
            else if((res = get_conn_long())!= -1)
            {
                break;
            }
            else if((res = add_conn()) != -1)
            {
                break;
            }
        }
        usleep(10000);
        return_conn_cas(res);
    }
    printf("%d %d\n", get, cash);
    return NULL;
}

int get_conn() 
{
    int i = 0;
    int res = -1;
    pthread_mutex_lock(&lock); 
    for (i = 0; i < POOL_MAX_CNT; i++)
    {
        if (!conn_lock_status[i])
        {
            conn_lock_status[i] = 1;
            res = i;
            break;
        }
    }
    pthread_mutex_unlock(&lock); // 🔓 해제
    return res;
}

int get_conn_long() 
{
    int i = rand()%POOL_MAX_CNT;
    for (int j = 0; j < POOL_MAX_CNT; j++) 
    {
        i++;
        if(i == POOL_MAX_CNT)
            i = 0; 
        if(__sync_bool_compare_and_swap(&conn_cas_status[i].value, 0, 1)) 
        {
            pthread_setspecific(key, (void *)(intptr_t)i);
            conn_cas_status[i].flag = IN_USING;
            conn_cas_status[i].updated = now;
            //printf("%lu use %d\n", (unsigned long)pthread_self(), i);
            return i;
        }
    }
    return -1;
}

int get_conn_short() 
{
    void *ptr = pthread_getspecific(key);
    if(ptr)
    {
        int index = (int)(intptr_t)ptr;
        if(__sync_bool_compare_and_swap(&conn_cas_status[index].value, 0, 1)) 
        {
            pthread_setspecific(key, (void *)(intptr_t)index);
            conn_cas_status[index].flag = IN_USING;
            conn_cas_status[index].updated = now;
            //printf("%lu use short %d\n", (unsigned long)pthread_self(), index);
            return index;
        }
    }
    return -1;
}

int add_conn()
{
    int i = 0;
    for(i = 0; i < POOL_MAX_CNT; i++)
    {
        //대충 커넥션 만드는 로직 
        if(__sync_bool_compare_and_swap(&conn_cas_status[i].flag, CLOSED, IN_USING))
        {
            //언제 할당했는지 정보 필요
            pthread_setspecific(key, (void *)(intptr_t)i);
            conn_cas_status[i].updated = now;       //매번 시간 계산 대신 하우스 키퍼 계산한 시간 사용
            conn_cas_status[i].created = now;
            printf("addconn deetected %d\n", i);
            return i;
        }
    }
    return -1;
}


void return_conn(int i)
{
    pthread_mutex_lock(&lock); 
    conn_lock_status[i] = 0;
    //끊어진경우 그대로 두고 flag 변경

    pthread_mutex_unlock(&lock);
}

void return_conn_cas(int i)
{
    //반납때 오래됬으면 제거
    if(is_valid(i) && conn_cas_status[i].created + 30 * 60 > now)
    {
        //커넥션이 멀정하면
        conn_cas_status[i].updated = now;
        __sync_bool_compare_and_swap(&conn_cas_status[i].value, 1, 0);
    }
    else
    {
        //커넥션 닫기
        printf("%ld %ld conn close\n", now, conn_cas_status[i].created );
        conn_cas_status[i].flag = CLOSED;
    }
}


int main() 
{
    srand(time(NULL));
    pthread_key_create(&key, NULL);
    struct timeval start, end;
    long elapsed;
    pthread_t thread_lock[THREAD_CNT];
    pthread_t thread_cas[THREAD_CNT];
    pthread_t hk;
    pthread_mutex_init(&lock, NULL); // 뮤텍스 초기화
    int i = 0;

    gettimeofday(&start, NULL);
    for(i =0; i < TRY; i++)
        usleep(10000);

    gettimeofday(&end, NULL);
    
    elapsed = (end.tv_sec - start.tv_sec) * 1000000L + (end.tv_usec - start.tv_usec);

    printf("sleep 실행 시간: %ld 마이크로초 (%.3f초)\n", elapsed, elapsed / 1000000.0);


    gettimeofday(&start, NULL);

    for(i = 0; i< THREAD_CNT; i++)
    {
        //pthread_create(&thread_lock[i], NULL, worker_lock, NULL);
    }

    
    for (i = 0; i < THREAD_CNT; i++) 
    {
        //pthread_join(thread_lock[i], NULL);
    }

    gettimeofday(&end, NULL);
    
    elapsed = (end.tv_sec - start.tv_sec) * 1000000L + (end.tv_usec - start.tv_usec);

    printf("MUTEX 실행 시간: %ld 마이크로초 (%.3f초)\n", elapsed, elapsed / 1000000.0);

    for(i = 0; i < POOL_MAX_CNT; i++)
    {
        if(i < POOL_MIN_CNT)
        {
            conn_cas_status[i].value = 0;
            conn_cas_status[i].updated = time(NULL);
            conn_cas_status[i].created = time(NULL);
            conn_cas_status[i].flag = IN_USING;
        }
        else
        {
            conn_cas_status[i].value = 1;
            conn_cas_status[i].updated = 0;
            conn_cas_status[i].created = 0;
            conn_cas_status[i].flag = CLOSED;            
        }
        //printf("%ld %ld\n", conn_cas_status[i].created, conn_cas_status[i].updated);
    }

    pthread_create(&hk, NULL, house_keeper, NULL);
    gettimeofday(&start, NULL);

    for(i = 0; i< THREAD_CNT; i++)
    {
        pthread_create(&thread_cas[i], NULL, worker_cas, NULL);
    }

    
    for (i = 0; i < THREAD_CNT; i++) 
    {
        pthread_join(thread_cas[i], NULL);
    }

    gettimeofday(&end, NULL);
    //pthread_join(hk, NULL);
    elapsed = (end.tv_sec - start.tv_sec) * 1000000L + (end.tv_usec - start.tv_usec);
    printf("CAS 실행 시간: %ld 마이크로초 (%.3f초)\n", elapsed, elapsed / 1000000.0);

    pthread_mutex_destroy(&lock); // 뮤텍스 제거
    pthread_key_delete(key);
    return 0;
}

/*
* 커넥션 확인 함수
* ex) pq_status()
*/
int is_valid(int index)
{
    return 1;
}