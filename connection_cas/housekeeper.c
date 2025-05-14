#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#include <assert.h> // test code
#include <unistd.h> // sleep 함수용

#include <libpq-fe.h>

#define MAX_HASH_SIZE   256
#define MAX_QUE_SIZE     10
#define CONN_SIZE        10
#define FAIL             -1
#define SUCCESS           0

#define TRUE              1
#define FALSE             0

typedef int             bool;



typedef struct 
{
    pthread_cond_t que_cond[MAX_QUE_SIZE];
    pthread_mutex_t mutex;
    int front;
    int rear;
    int count;

} wait_que;

typedef struct entry_st 
{
    unsigned long key;
    int value;
    int delete_flag;
    struct entry_st *next;
    struct entry_st *next_trash;
} entry_st;

typedef struct 
{
    entry_st *bucket[MAX_HASH_SIZE];
    bool bucket_use[MAX_HASH_SIZE];
    entry_st *que_start;
    entry_st *que_end;
}hash_map;

/*
    락 범위 엔트리 단위 vs 버킷 단위
    버킷 단위로 해야 삭제힐떼 편함

    GET은 락 없어야하는데 읽을때 수정한다면? -> RCU? hazard 뭐 어쨰?
    읽을떄도 락 없이 읽을수 있어야한다
    일단은 스레드 세이프 하지 않게 만들고 생각해보자
    250507완료료

    버킷 단위로 락 추가? -> GC 돌떄 편함
    엔트리 단위로 cas -> 캐시 지역성 고려해봐야함
    삭제는 소프트하게 하고 나중에 락 획득 후 GC 
*/



/*
    읽기와 논리적 삭제에 락 없이 하는법
    삭제 시 CAS로 다른쓰레드에서도 해당 변경값 읽을 수있도록 함

    물리적 삭제시 read 에 락이 없으면 할당 해제시  겹칠수 있음
    따라서 바로 삭제할수 없음
    
    버킷 수만큼 연결 리스트로 된 queue을 삭제할 엔트리 저장용으로 만듬듬 
    엔트리에 삭제용 큐잉 할수 있는 주소 추가
    GC를 돌면 이전 엔트리에서 현재 엔트리의 넥스트 엔트릐의 주소로 변경후 
    큐에 삭제 대상 엔트리 삭입
    이러면 이제 삭제 엔트리에 진입 할수 없음
    충분한 시간이 지나면 락 없이 큐를 읽으면서 전부 삭제
*/

typedef struct 
{
    PGconn *conn_list[CONN_SIZE];
    hash_map map;
    wait_que que;
} conn_pool;


void queue_init(wait_que *q);
void queue_destroy(wait_que *q);
int enque(wait_que *q);
int deque(wait_que *q);

unsigned int hash(unsigned long tid);
void hash_init(hash_map *map);
void hash_free(hash_map *map);
int hash_insert(hash_map * map, unsigned long tid, int value);
int hash_get(hash_map * map, unsigned long tid);
int hash_delete(hash_map * map);
int hash_delete_soft(hash_map * map, unsigned long tid);

void* thread_func(void* arg);
void que_test();
void hash_test();

int get_lock(hash_map *map, int index);
int release_lock(hash_map *map, int index);

void insert_trash(hash_map *map, entry_st *entry);
void clean_trash(hash_map *map);
wait_que g_que;

int main()
{
    //que_test();
    hash_test();
    return 0;
}


void queue_init(wait_que *q)
{
    int i = 0;
    q->front = 0;
    q->rear = 0;
    q->count = 0;
    pthread_mutex_init(&q->mutex, NULL);

    for(i = 0;i < MAX_QUE_SIZE; i++)
        pthread_cond_init(&q->que_cond[i], NULL);
}

void queue_destroy(wait_que *q)
{
    int i = 0;
    pthread_mutex_destroy(&q->mutex);
    for(i = 0;i < MAX_QUE_SIZE; i++)
        pthread_cond_destroy(&q->que_cond[i]);
}

int enque(wait_que *q)
{
    int index = 0;
    int conn_index = 0;
    
    pthread_mutex_lock(&q->mutex);
    if(q->count == MAX_QUE_SIZE)
    {
        pthread_mutex_unlock(&q->mutex);
        return FAIL;
    }

    index = q->rear;
    q->rear = (q->rear + 1) % MAX_QUE_SIZE;
    q->count++;
    pthread_cond_wait(&q->que_cond[index], &q->mutex);
    pthread_mutex_unlock(&q->mutex);
    
    //conn_full_check();
    return conn_index; // conn_index;
}

int deque(wait_que *q)
{
    int index = 0;
    pthread_mutex_lock(&q->mutex);
    if(q->count == 0)
    {
        pthread_mutex_unlock(&q->mutex);
        return FAIL;
    }

    index = q->front;
    pthread_cond_signal(&q->que_cond[index]);
    q->front = (q->front + 1) % MAX_QUE_SIZE;
    q->count--;
    pthread_mutex_unlock(&q->mutex);
    return SUCCESS;
}


void hash_init(hash_map *map)
{
    int i = 0;
    for (i = 0; i < MAX_HASH_SIZE; i ++)
    {
        map->bucket[i] = NULL;
        map->bucket_use[i] = FALSE;
    }
    map->que_start = NULL;
    map->que_end = NULL;
}

wait_que g_que;
hash_map map;

void* thread_func(void* arg) 
{
    printf("enque\n");
    enque(&g_que);
    return NULL;
}

void que_test()
{
    pthread_t thread1, thread2;

    queue_init(&g_que); 
    printf("que_init \n");

    pthread_create(&thread1, NULL, thread_func, NULL);
    pthread_create(&thread2, NULL, thread_func, NULL);
    
    sleep(1);

    deque(&g_que);
    printf("deque\n");
    deque(&g_que);
    printf("deque\n");


    pthread_join(thread1, NULL);
    pthread_join(thread2, NULL);    
    printf("Main thread: All threads finished.\n");
}

void hash_test()
{
    hash_init(&map);
    hash_insert(&map, 1UL, 1);
    assert(hash_get(&map, 1UL) == 1);

    hash_insert(&map, 1UL, 2);
    assert(hash_get(&map, 1UL) == 2);

    hash_delete_soft(&map, 1UL);
    assert(hash_get(&map, 1UL) == FAIL);

    hash_insert(&map, 2UL, 3);
    assert(hash_get(&map, 2UL) == 3);
    printf("hash test finish!\n");
}

unsigned int hash(unsigned long tid)
{
    //return ((uintptr_t) tid) % MAX_HASH_SIZE;
    return tid % MAX_HASH_SIZE;
}

//마지막 엔트리 CAS 획득 필요
//d_flag 이면 넣기
int hash_insert(hash_map *map, unsigned long tid, int value)
{
    int index = hash(tid);

    entry_st *new_entry = NULL;
    new_entry = (entry_st *)malloc(sizeof(entry_st));
    if (! new_entry)return FAIL;

    new_entry->key = tid;
    new_entry->value = value;
    new_entry->next = NULL;
    new_entry->delete_flag = FALSE;
    
    get_lock(map, index);
    entry_st *entry = map->bucket[index];
    if (entry == NULL)
    {
        map->bucket[index] = new_entry;
        release_lock(map, index);
        return SUCCESS;
    }

    while(entry)
    {
        if(entry->key == new_entry->key && !entry->delete_flag )
        {
            entry->value = value;
            free(new_entry);
            release_lock(map, index);
            return SUCCESS;
        }
        else if (entry->next == NULL)
        {
            entry->next = new_entry;
            release_lock(map, index);
            return SUCCESS;
        }
    }
    release_lock(map, index);

    return FAIL;
}

int hash_get(hash_map * map, unsigned long tid)
{
    int index = hash(tid);
    entry_st *entry = map->bucket[index];
    while (entry) 
    {
        if (tid == entry->key && !entry->delete_flag)
        {
            return entry->value;
        }
        entry = entry->next;
    }
    return FAIL;  // 키를 찾지 못한 경우

}


//prev, entry cas 획득 -> GC 용
int hash_delete(hash_map *map)
{
    entry_st *entry = NULL;
    entry_st *prev = NULL;
    int index = 0;
    int i = 0;
    for (i = 0; i < MAX_HASH_SIZE; i++)
    {

        
        entry_st *entry = map->bucket[index];
        get_lock(map, index);
        while (entry) 
        {
            if (entry->delete_flag)
            {
                
                if (prev) 
                {
                    prev->next = entry->next;
                }
                else 
                {
                    map->bucket[index] = entry->next;
                }
                insert_trash(map, entry);
                //free(entry); 읽고 있을수 있음!
                
                entry = entry->next;
            }
            prev = entry;
            entry = entry->next;
        }
        release_lock(map, index);
    }
    sleep(1);
    clean_trash(map);
    return SUCCESS; 
}

int hash_delete_soft(hash_map *map, unsigned long tid)
{
    int index = hash(tid);
    entry_st *entry = map->bucket[index];

    while (entry) 
    {
        if (tid == entry->key)
        {
            //메모리 가시성
            __sync_bool_compare_and_swap(&entry->delete_flag, FALSE, TRUE);
            return SUCCESS;
        }
        entry = entry->next;
    }
    return FAIL;  // 키를 찾지 못한 경우
}


int get_lock(hash_map *map, int index)
{
    while (!__sync_bool_compare_and_swap(&map->bucket_use[index], FALSE, TRUE));
    return SUCCESS;
}
int release_lock(hash_map *map, int index)
{
    map->bucket_use[index] = FALSE;
    return SUCCESS;
}

void insert_trash(hash_map *map, entry_st *entry)
{
    entry_st *last_entrty = NULL;
    if(!map->que_start)
    {
        map->que_start = entry;
        map->que_end = entry;
    }
    else
    {
        last_entrty = map->que_end;
        last_entrty->next_trash = entry;
        map->que_end = entry;
    }
}
void clean_trash(hash_map *map)
{
    entry_st *entry = map->que_start;
    entry_st *next = NULL;
    while(entry)
    {
        next = entry->next_trash;
        free(entry);
    }
}