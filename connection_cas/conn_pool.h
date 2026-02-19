#ifndef CONN_POOL_H
#define CONN_POOL_H

#include <libpq-fe.h>
#include "thread_safe_queue.h"

#define CONN_SIZE   10

enum conn_flag
{
    NONE = 0,
    IN_USING,
    BROKEN,
    CHECKING,
    CLOSE
};

enum state_flag
{
    CONN_AVAILABLE = 0,
    CONN_UNAVAILABLE
};

typedef struct
{
    PGconn   *conn_list[CONN_SIZE];
    int       state[CONN_SIZE];   // CAS로 점유 관리 (AVAILABLE/UNAVAILABLE)
    hash_map *map;                // tid → conn index 캐시
    wait_que *que;                // 풀 고갈 시 대기 큐
    char      connect_info[1024];
} conn_pool;

PGconn *get_conn(conn_pool *pool);
void    release_conn(conn_pool *pool, PGconn *conn);

#endif // CONN_POOL_H
