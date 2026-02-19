#include "conn_pool.h"

PGconn *get_conn(conn_pool *pool)
{
    int i = 0;
    int index = -1;
    unsigned long tid = (unsigned long)pthread_self();

    index = hash_get(pool->map, tid);
    // fast path: 스레드 캐시된 인덱스로 바로 시도
    if(index != -1 && __sync_bool_compare_and_swap(&pool->state[index], CONN_AVAILABLE, CONN_UNAVAILABLE))
        return pool->conn_list[index];

    // slow path: 풀 전체 순회
    for(i = 0; i < CONN_SIZE; i++)
    {
        if(!__sync_bool_compare_and_swap(&pool->flag[i], CONN_AVAILABLE, CONN_UNAVAILABLE))
            continue;
        return pool->conn_list[i];
    }

    return NULL;
}
