#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>

#define THREAD_CNT 80
#define POOL_CNT 100
#define TRY 100000
#define SLEEP 30000

struct aligned_int {
    volatile int value;
    char padding[60]; // 64바이트 정렬
} __attribute__((aligned(64)));


int get_conn();
int get_conn_cas();

void return_conn(int index);
void return_conn_cas(int index);

pthread_mutex_t lock; // 전역 뮤텍스


int conn_lock_status[POOL_CNT] = {0,};
struct aligned_int conn_cas_status[POOL_CNT];



void *worker_lock(void *arg) 
{
    int res = 0;
    for (int i = 0; i < TRY; i++) {
        res = get_conn();
        if (res != -1)
        {
            //usleep(SLEEP);
            return_conn(res);
        }

    }
    return NULL;
}

void *worker_cas(void *arg) 
{
    int res = 0;
    for (int i = 0; i < TRY; i++) {
        res = get_conn_cas();

        if (res != -1)
        {
            //usleep(SLEEP);
            return_conn_cas(res);
        }

    }
    return NULL;
}

int get_conn() 
{
    int i = 0;
    int res = -1;
    pthread_mutex_lock(&lock); 
    for (i = 0; i < POOL_CNT; i++)
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

int get_conn_cas() 
{
    for (int i = 0; i < POOL_CNT; i++) 
    {
        if(__sync_bool_compare_and_swap(&conn_cas_status[i].value, 0, 1)) 
        {
            return i;
        }
    }
    return -1;
}


void return_conn(int i)
{
    pthread_mutex_lock(&lock); 
    conn_lock_status[i] = 0;
    pthread_mutex_unlock(&lock);
}

void return_conn_cas(int i)
{
    __sync_bool_compare_and_swap(&conn_cas_status[i].value, 1, 0);
    
}


int main() 
{
    struct timeval start, end;
    long elapsed;
    pthread_t thread_lock[THREAD_CNT];
    pthread_t thread_cas[THREAD_CNT];
    
    pthread_mutex_init(&lock, NULL); // 뮤텍스 초기화
    int i = 0;


    gettimeofday(&start, NULL);

    for(i = 0; i< THREAD_CNT; i++)
    {
        pthread_create(&thread_lock[i], NULL, worker_lock, NULL);
    }

    
    for (i = 0; i < THREAD_CNT; i++) 
    {
        pthread_join(thread_lock[i], NULL);
    }

    gettimeofday(&end, NULL);
    
    elapsed = (end.tv_sec - start.tv_sec) * 1000000L + (end.tv_usec - start.tv_usec);

    printf("MUTEX 실행 시간: %ld 마이크로초 (%.3f초)\n", elapsed, elapsed / 1000000.0);



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
    
    elapsed = (end.tv_sec - start.tv_sec) * 1000000L + (end.tv_usec - start.tv_usec);
    printf("CAS 실행 시간: %ld 마이크로초 (%.3f초)\n", elapsed, elapsed / 1000000.0);

    pthread_mutex_destroy(&lock); // 뮤텍스 제거
    return 0;
}


