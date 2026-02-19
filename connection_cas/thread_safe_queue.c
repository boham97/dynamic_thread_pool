#include "thread_safe_queue.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

// ─── wait_que ───────────────────────────────────────────────

void queue_init(wait_que *q)
{
    int i = 0;
    q->front = 0;
    q->rear = 0;
    q->count = 0;
    pthread_mutex_init(&q->mutex, NULL);

    for(i = 0; i < MAX_QUE_SIZE; i++)
        pthread_cond_init(&q->que_cond[i], NULL);
}

void queue_destroy(wait_que *q)
{
    int i = 0;
    pthread_mutex_destroy(&q->mutex);
    for(i = 0; i < MAX_QUE_SIZE; i++)
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

    return conn_index;
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

// ─── hash_map ───────────────────────────────────────────────

void hash_init(hash_map *map)
{
    int i = 0;
    for (i = 0; i < MAX_HASH_SIZE; i++)
    {
        map->bucket[i] = NULL;
        map->bucket_use[i] = FALSE;
    }
    map->que_start = NULL;
    map->que_end = NULL;
}

static int get_lock(hash_map *map, int index)
{
    while (!__sync_bool_compare_and_swap(&map->bucket_use[index], FALSE, TRUE));
    return SUCCESS;
}

static int release_lock(hash_map *map, int index)
{
    map->bucket_use[index] = FALSE;
    return SUCCESS;
}

unsigned int hash(unsigned long tid)
{
    return tid % MAX_HASH_SIZE;
}

int hash_insert(hash_map *map, unsigned long tid, int value)
{
    int index = hash(tid);

    entry_st *new_entry = (entry_st *)malloc(sizeof(entry_st));
    if (!new_entry) return FAIL;

    new_entry->key = tid;
    new_entry->value = value;
    new_entry->next = NULL;
    new_entry->next_trash = NULL;
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
        if(entry->key == new_entry->key)
        {
            // 살아있는 노드: 값 갱신
            // 삭제된 노드: delete_flag 해제 후 재활용
            entry->value = value;
            entry->delete_flag = FALSE;
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
        entry = entry->next;
    }
    release_lock(map, index);
    return FAIL;
}

int hash_get(hash_map *map, unsigned long tid)
{
    int index = hash(tid);
    entry_st *entry = map->bucket[index];
    while (entry)
    {
        if (tid == entry->key && !entry->delete_flag)
            return entry->value;
        entry = entry->next;
    }
    return FAIL;
}

int hash_delete_soft(hash_map *map, unsigned long tid)
{
    int index = hash(tid);
    entry_st *entry = map->bucket[index];
    while (entry)
    {
        if (tid == entry->key)
        {
            __sync_bool_compare_and_swap(&entry->delete_flag, FALSE, TRUE);
            return SUCCESS;
        }
        entry = entry->next;
    }
    return FAIL;
}

static void insert_trash(hash_map *map, entry_st *entry)
{
    if(map->que_start)
    {
        map->que_end->next_trash = entry;
        map->que_end = entry;
    }
    else
    {
        map->que_start = entry;
        map->que_end = entry;
    }
}

static void clean_trash(hash_map *map)
{
    entry_st *entry = map->que_start;
    entry_st *next = NULL;
    while(entry)
    {
        next = entry->next_trash;
        free(entry);
        entry = next;
    }
    map->que_start = NULL;
    map->que_end = NULL;
}

int hash_delete(hash_map *map)
{
    entry_st *prev = NULL;
    int i = 0;
    for (i = 0; i < MAX_HASH_SIZE; i++)
    {
        prev = NULL;
        get_lock(map, i);
        entry_st *entry = map->bucket[i];
        while (entry)
        {
            if (entry->delete_flag)
            {
                entry_st *next = entry->next;
                if (prev)
                    prev->next = next;
                else
                    map->bucket[i] = next;
                insert_trash(map, entry);
                entry = next;
            }
            else
            {
                prev = entry;
                entry = entry->next;
            }
        }
        release_lock(map, i);
    }
    sleep(1);
    clean_trash(map);
    return SUCCESS;
}

int hash_get_all(hash_map *map)
{
    int i = 0;
    for (i = 0; i < MAX_HASH_SIZE; i++)
    {
        get_lock(map, i);
        entry_st *entry = map->bucket[i];
        while (entry)
        {
            printf("hash_get_all: key=%lu value=%d deleted=%d\n",
                   entry->key, entry->value, entry->delete_flag);
            entry = entry->next;
        }
        release_lock(map, i);
    }
    return SUCCESS;
}
