#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>

int get_conn();
int get_conn_cas();
void return_conn(int index);
void return_conn_cas(int index);


pthread_mutex_t lock; // 전역 뮤텍스

int counter = 0;

int conn_lock_status[50] = {0,};
int conn_cas_status[50] = {0,};



void *worker_lock(void *arg) 
{
    int res = 0;
    for (int i = 0; i < 100000; i++) {
        res = get_conn();
        if (res != -1)
        {
            counter++;
            usleep(100);
            return_conn(res);
        }

    }
    return NULL;
}

void *worker_cas(void *arg) 
{
    int res = 0;
    for (int i = 0; i < 100000; i++) {
        res = get_conn_cas();
        if (res != -1)
        {
            counter++;
            usleep(10);
            return_conn_cas(res);
        }

    }
    return NULL;
}

int get_conn() 
{
    int i = 0;
    pthread_mutex_lock(&lock); 
    for (i = 0; i < 50; i++)
    {
        if (conn_lock_status[i] == 0)
        {
            conn_lock_status[i] = 1;
            pthread_mutex_unlock(&lock); // 🔓 해제
            return i;
        }
    }
    pthread_mutex_unlock(&lock); // 🔓 해제
    return -1;
}

int get_conn_cas() 
{
    int i = 0;
    for (i = 0; i < 50; i++)
    {
        if(__sync_bool_compare_and_swap(&conn_lock_status[i], 0, 1))
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
    __sync_bool_compare_and_swap(&conn_lock_status[i], 1, 0);
    
}


int main() 
{
    struct timeval start, end;
    long elapsed;
    pthread_t thread_lock[10];
    pthread_t thread_cas[10];

    pthread_mutex_init(&lock, NULL); // 뮤텍스 초기화
    int i = 0;


    gettimeofday(&start, NULL);

    for(i = 0; i< 10; i++)
    {
        pthread_create(&thread_lock[i], NULL, worker_lock, NULL);
    }

    
    for (i = 0; i < 10; i++) 
    {
        pthread_join(thread_lock[i], NULL);
    }

    gettimeofday(&end, NULL);
    
    elapsed = (end.tv_sec - start.tv_sec) * 1000000L + (end.tv_usec - start.tv_usec);

    printf("실행 시간: %ld 마이크로초 (%.3f초)\n", elapsed, elapsed / 1000000.0);

    printf("Counter: %d\n", counter);
    counter = 0;


    gettimeofday(&start, NULL);

    for(i = 0; i< 10; i++)
    {
        pthread_create(&thread_cas[i], NULL, worker_cas, NULL);
    }

    
    for (i = 0; i < 10; i++) 
    {
        pthread_join(thread_cas[i], NULL);
    }

    gettimeofday(&end, NULL);
    
    elapsed = (end.tv_sec - start.tv_sec) * 1000000L + (end.tv_usec - start.tv_usec);

    pthread_mutex_destroy(&lock); // 뮤텍스 제거
    return 0;
}


