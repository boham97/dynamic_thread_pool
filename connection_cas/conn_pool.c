#include "conn_pool.h"

PGconn *get_conn(conn_pool *pool)
{
    int i = 0;
    int index = -1;
    unsigned long tid = (unsigned long)pthread_self();

    // fast path: 이 스레드가 마지막으로 쓴 커넥션 인덱스 캐시 조회
    index = hash_get(pool->map, tid);
    if(index != -1 && __sync_bool_compare_and_swap(&pool->state[index], CONN_AVAILABLE, CONN_UNAVAILABLE))
        return pool->conn_list[index];

    // slow path: 풀 전체 순회
    for(i = 0; i < CONN_SIZE; i++)
    {
        if(!__sync_bool_compare_and_swap(&pool->state[i], CONN_AVAILABLE, CONN_UNAVAILABLE))
            continue;

        // 찾은 인덱스를 해시맵에 캐싱 → 다음 요청은 fast path로
        hash_insert(pool->map, tid, i);
        return pool->conn_list[i];
    }

    // 풀 고갈: wait_que에 대기 후 깨어나면 재시도
    enque(pool->que);
    return get_conn(pool);
}

// ─── get_conn_2: TLS로 fast path 캐시 ────────────────────────

static __thread int tls_conn_index = -1;

PGconn *get_conn_2(conn_pool *pool)
{
    int i;

    // fast path: TLS에 캐싱된 인덱스로 바로 시도
    if (tls_conn_index != -1 &&
        __sync_bool_compare_and_swap(&pool->state[tls_conn_index], CONN_AVAILABLE, CONN_UNAVAILABLE))
        return pool->conn_list[tls_conn_index];

    // slow path: 풀 전체 순회
    for (i = 0; i < CONN_SIZE; i++)
    {
        if (!__sync_bool_compare_and_swap(&pool->state[i], CONN_AVAILABLE, CONN_UNAVAILABLE))
            continue;

        tls_conn_index = i;
        return pool->conn_list[i];
    }

    // 풀 고갈: wait_que에 대기 후 재시도
    enque(pool->que);
    return get_conn_2(pool);
}

void release_conn(conn_pool *pool, PGconn *conn)
{
    int i = 0;
    for(i = 0; i < CONN_SIZE; i++)
    {
        if(pool->conn_list[i] != conn)
            continue;

        __sync_bool_compare_and_swap(&pool->state[i], CONN_UNAVAILABLE, CONN_AVAILABLE);

        // 대기 중인 스레드가 있으면 하나 깨움
        deque(pool->que);
        return;
    }
}
