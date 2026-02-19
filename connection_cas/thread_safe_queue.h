#ifndef THREAD_SAFE_QUEUE_H
#define THREAD_SAFE_QUEUE_H

#include <pthread.h>

#define MAX_HASH_SIZE   256
#define MAX_QUE_SIZE     10
#define FAIL             -1
#define SUCCESS           0
#define TRUE              1
#define FALSE             0

typedef int bool;

typedef struct
{
    pthread_cond_t  que_cond[MAX_QUE_SIZE];
    pthread_mutex_t mutex;
    int front;
    int rear;
    int count;
} wait_que;

typedef struct entry_st
{
    unsigned long    key;
    int              value;
    int              delete_flag;
    struct entry_st *next;
    struct entry_st *next_trash;
} entry_st;

typedef struct
{
    entry_st *bucket[MAX_HASH_SIZE];
    bool      bucket_use[MAX_HASH_SIZE];
    entry_st *que_start;
    entry_st *que_end;
} hash_map;

// wait_que
void queue_init(wait_que *q);
void queue_destroy(wait_que *q);
int  enque(wait_que *q);
int  deque(wait_que *q);

// hash_map
void         hash_init(hash_map *map);
unsigned int hash(unsigned long tid);
int          hash_insert(hash_map *map, unsigned long tid, int value);
int          hash_get(hash_map *map, unsigned long tid);
int          hash_delete(hash_map *map);
int          hash_delete_soft(hash_map *map, unsigned long tid);
int          hash_get_all(hash_map *map);

#endif // THREAD_SAFE_QUEUE_H
