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
    int       state[CONN_SIZE];
    int       flag[CONN_SIZE];
    hash_map *map;
    wait_que *que;
    char      connect_info[1024];
} conn_pool;

PGconn *get_conn(conn_pool *pool);

#endif // CONN_POOL_H
