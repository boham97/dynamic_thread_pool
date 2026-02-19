#include "thread_safe_queue.h"
#include "conn_pool.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>

// PGconn mock: 더미 구조체를 PGconn* 로 캐스팅해서 사용
typedef struct { int id; } PGconn_mock;

// ─── 단일 스레드 기본 동작 테스트 ────────────────────────────

void test_single_thread()
{
    hash_map map;
    hash_init(&map);

    // insert & get
    hash_insert(&map, 1UL, 1);
    assert(hash_get(&map, 1UL) == 1);

    // 같은 키 덮어쓰기
    hash_insert(&map, 1UL, 2);
    assert(hash_get(&map, 1UL) == 2);

    // soft delete 후 get은 FAIL
    hash_delete_soft(&map, 1UL);
    assert(hash_get(&map, 1UL) == FAIL);

    // soft delete 후 재insert
    hash_insert(&map, 1UL, 1);
    assert(hash_get(&map, 1UL) == 1);

    // 다른 키 추가
    hash_insert(&map, 2UL, 3);
    assert(hash_get(&map, 2UL) == 3);

    // key=2 soft delete 후 GC
    hash_delete_soft(&map, 2UL);
    hash_delete(&map);  // sleep(1) + clean_trash 포함

    // GC 후 key=1은 살아있고, key=2는 사라짐
    assert(hash_get(&map, 1UL) == 1);
    assert(hash_get(&map, 2UL) == FAIL);

    printf("[PASS] test_single_thread\n");
}

// ─── 멀티스레드 동시성 테스트 ────────────────────────────────

#define MT_THREADS  8
#define MT_ITER     2000

typedef struct {
    hash_map *map;
    int       thread_index;
    int       errors;
} worker_arg_t;

void* worker(void *arg)
{
    worker_arg_t *a = (worker_arg_t *)arg;
    unsigned long tid = (unsigned long)pthread_self();
    int i;

    for (i = 0; i < MT_ITER; i++)
    {
        // insert → get → 값 확인
        hash_insert(a->map, tid, a->thread_index);
        int v = hash_get(a->map, tid);
        // soft_delete 직후 다른 스레드의 GC와 겹칠 수 있으므로 FAIL도 허용
        if (v != a->thread_index && v != FAIL)
        {
            fprintf(stderr, "[ERR] thread %d: expected %d or FAIL, got %d\n",
                    a->thread_index, a->thread_index, v);
            a->errors++;
        }

        // 100회마다 soft_delete → 재insert
        if (i % 100 == 0)
        {
            hash_delete_soft(a->map, tid);
            // soft_delete 직후에는 반드시 FAIL
            assert(hash_get(a->map, tid) == FAIL);
            hash_insert(a->map, tid, a->thread_index);
        }
    }
    return NULL;
}

void test_multi_thread()
{
    hash_map map;
    hash_init(&map);

    pthread_t    threads[MT_THREADS];
    worker_arg_t args[MT_THREADS];
    int i;

    for (i = 0; i < MT_THREADS; i++)
    {
        args[i].map          = &map;
        args[i].thread_index = i + 1;
        args[i].errors       = 0;
        pthread_create(&threads[i], NULL, worker, &args[i]);
    }

    for (i = 0; i < MT_THREADS; i++)
        pthread_join(threads[i], NULL);

    // 전체 오류 집계
    int total_errors = 0;
    for (i = 0; i < MT_THREADS; i++)
        total_errors += args[i].errors;

    // 모든 스레드 종료 후 GC
    hash_delete(&map);

    if (total_errors == 0)
        printf("[PASS] test_multi_thread (%d threads x %d iter)\n",
               MT_THREADS, MT_ITER);
    else
        printf("[FAIL] test_multi_thread: %d errors\n", total_errors);
}

// ─── 해시 충돌 테스트 ─────────────────────────────────────────
// MAX_HASH_SIZE=256 이므로 key=0, 256, 512 는 같은 버킷에 들어감

void test_hash_collision()
{
    hash_map map;
    hash_init(&map);

    unsigned long k1 = 0UL;
    unsigned long k2 = (unsigned long)MAX_HASH_SIZE;
    unsigned long k3 = (unsigned long)MAX_HASH_SIZE * 2;

    hash_insert(&map, k1, 10);
    hash_insert(&map, k2, 20);
    hash_insert(&map, k3, 30);

    assert(hash_get(&map, k1) == 10);
    assert(hash_get(&map, k2) == 20);
    assert(hash_get(&map, k3) == 30);

    hash_delete_soft(&map, k2);
    assert(hash_get(&map, k1) == 10);
    assert(hash_get(&map, k2) == FAIL);
    assert(hash_get(&map, k3) == 30);

    hash_delete(&map);

    assert(hash_get(&map, k1) == 10);
    assert(hash_get(&map, k2) == FAIL);
    assert(hash_get(&map, k3) == 30);

    printf("[PASS] test_hash_collision\n");
}

// ─── conn_pool 헬퍼 ──────────────────────────────────────────

static hash_map g_map;
static wait_que g_que;

static conn_pool* make_mock_pool(PGconn_mock *mocks, int n)
{
    conn_pool *pool = calloc(1, sizeof(conn_pool));
    int i;
    for(i = 0; i < n; i++)
        pool->conn_list[i] = (PGconn *)&mocks[i];
    pool->map = &g_map;
    pool->que = &g_que;
    hash_init(pool->map);
    queue_init(pool->que);
    return pool;
}

// ─── conn_pool 단일 스레드 테스트 ────────────────────────────

void test_conn_single()
{
    PGconn_mock mocks[CONN_SIZE];
    conn_pool *pool = make_mock_pool(mocks, CONN_SIZE);
    int i;

    // 커넥션 전부 획득
    PGconn *conns[CONN_SIZE];
    for(i = 0; i < CONN_SIZE; i++)
    {
        conns[i] = get_conn(pool);
        assert(conns[i] != NULL);
    }

    // 전부 반환
    for(i = 0; i < CONN_SIZE; i++)
        release_conn(pool, conns[i]);

    // 반환 후 재획득 가능한지 확인
    PGconn *c = get_conn(pool);
    assert(c != NULL);
    release_conn(pool, c);

    // fast path 확인: 같은 스레드가 재요청 시 같은 커넥션이 와야 함
    PGconn *c1 = get_conn(pool);
    release_conn(pool, c1);
    PGconn *c2 = get_conn(pool);
    assert(c1 == c2);
    release_conn(pool, c2);

    free(pool);
    printf("[PASS] test_conn_single\n");
}

// ─── conn_pool 멀티스레드 테스트 ─────────────────────────────

#define CP_THREADS  16
#define CP_ITER     500

typedef struct {
    conn_pool *pool;
    int        errors;
} cp_worker_arg_t;

void* cp_worker(void *arg)
{
    cp_worker_arg_t *a = (cp_worker_arg_t *)arg;
    int i;

    for(i = 0; i < CP_ITER; i++)
    {
        PGconn *c = get_conn(a->pool);
        if(c == NULL)
        {
            fprintf(stderr, "[ERR] get_conn returned NULL\n");
            a->errors++;
            continue;
        }
        // 커넥션 사용 중 simulate
        usleep(100);
        release_conn(a->pool, c);
    }
    return NULL;
}

void test_conn_multi()
{
    PGconn_mock mocks[CONN_SIZE];
    conn_pool *pool = make_mock_pool(mocks, CONN_SIZE);

    pthread_t       threads[CP_THREADS];
    cp_worker_arg_t args[CP_THREADS];
    int i;

    for(i = 0; i < CP_THREADS; i++)
    {
        args[i].pool   = pool;
        args[i].errors = 0;
        pthread_create(&threads[i], NULL, cp_worker, &args[i]);
    }

    for(i = 0; i < CP_THREADS; i++)
        pthread_join(threads[i], NULL);

    int total_errors = 0;
    for(i = 0; i < CP_THREADS; i++)
        total_errors += args[i].errors;

    free(pool);

    if(total_errors == 0)
        printf("[PASS] test_conn_multi (%d threads x %d iter, pool=%d)\n",
               CP_THREADS, CP_ITER, CONN_SIZE);
    else
        printf("[FAIL] test_conn_multi: %d errors\n", total_errors);
}

// ─── main ────────────────────────────────────────────────────

int main()
{
    test_single_thread();
    test_hash_collision();
    test_multi_thread();
    test_conn_single();
    test_conn_multi();
    return 0;
}
