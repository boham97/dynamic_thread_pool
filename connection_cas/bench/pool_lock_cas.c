#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>

#define THREAD_CNT 25
#define POOL_MIN_CNT 20
#define POOL_MAX_CNT 100
#define TRY 100000
#define SLEEP 30000


//점유 플래그
enum conn_flag
{
    NONE = 0,
    IN_USING,
    CHECKING,
    CLOSED
};


pthread_key_t key;
volatile int conn_cnt = POOL_MIN_CNT;
struct aligned_int {
    int value;
    long created; //커넥션 생성 시간 -> 오래 돠면 정리후 재 연결
    enum conn_flag flag; //점유 상황
    char padding[52]; // 64바이트 정렬
} __attribute__((aligned(64)));

int add_conn();
int get_conn();
int get_conn_long();
int get_conn_short();

void return_conn(int index);
void return_conn_cas(int index);
void conn_check(int i);
pthread_mutex_t lock; // 전역 뮤텍스


int conn_lock_status[POOL_MAX_CNT] = {0,};
struct aligned_int conn_cas_status[POOL_MAX_CNT];

void *house_keeper(void *arg)
{
    int i = 0;
    int conn_count = 0;
    for (int i = 0; i < conn_cnt; i++) 
    {
        if(__sync_bool_compare_and_swap(&conn_cas_status[i].value, 0, 1)) 
        {
            conn_check(i);
        }
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
                return_conn_cas(res);
                break;;
            }
            else if((res = get_conn_long())!= -1)
            {
                return_conn_cas(res);
                break;
            }
            else if((res = add_conn()) != -1)
            {
                return_conn_cas(res);
                break;
            }
        }
    }
    printf("%d %d\n", get, cash);
    return NULL;
}

int get_conn() 
{
    int i = 0;
    int res = -1;
    pthread_mutex_lock(&lock); 
    for (i = 0; i < POOL_MIN_CNT; i++)
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
    for (int i = 0; i < conn_cnt; i++) 
    {
        if(__sync_bool_compare_and_swap(&conn_cas_status[i].value, 0, 1)) 
        {
            pthread_setspecific(key, (void *)(intptr_t)i);
            conn_cas_status[i].flag = IN_USING;
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
            return index;
        }
    }
    return -1;
}

int add_conn()
{
    int i = 0;
    for(i = POOL_MIN_CNT; i < POOL_MAX_CNT; i++)
    {
        //대충 커넥션 만드는 로직 
        if(__sync_bool_compare_and_swap(&conn_cas_status[i].value, 1, 1))
        {
            //커넥션 할당
            conn_cas_status[i].flag = IN_USING;
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
    __sync_bool_compare_and_swap(&conn_cas_status[i].value, 1, 0);
    conn_cas_status[i].flag = NONE;
}

void conn_check(int i)
{
    //1. 오래된 커넥션 끊고 
}

int main() 
{
    
    pthread_key_create(&key, NULL);
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

    for(i = POOL_MIN_CNT; i < POOL_MAX_CNT; i++)
    {
        conn_cas_status[i].value = 1;
    }

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
    pthread_key_delete(key);
    return 0;
}


