#include "thread_safe_queue.h"
#include "conn_pool.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

// ─── 시간 측정 헬퍼 ──────────────────────────────────────────

static double elapsed_ms(struct timespec *s, struct timespec *e)
{
    return (e->tv_sec - s->tv_sec) * 1000.0
         + (e->tv_nsec - s->tv_nsec) / 1e6;
}

#define RUN_TIMED(label, block)                              \
    do {                                                     \
        struct timespec _s, _e;                              \
        clock_gettime(CLOCK_MONOTONIC, &_s);                 \
        block                                                \
        clock_gettime(CLOCK_MONOTONIC, &_e);                 \
        printf("  elapsed: %.2f ms\n", elapsed_ms(&_s, &_e));\
    } while(0)

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
    pthread_key_create(&pool->tls_key, NULL);
    return pool;
}

static void free_mock_pool(conn_pool *pool)
{
    pthread_key_delete(pool->tls_key);
    free(pool);
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

    free_mock_pool(pool);
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

    free_mock_pool(pool);

    if(total_errors == 0)
        printf("[PASS] test_conn_multi (%d threads x %d iter, pool=%d)\n",
               CP_THREADS, CP_ITER, CONN_SIZE);
    else
        printf("[FAIL] test_conn_multi: %d errors\n", total_errors);
}

// ─── 마이크로벤치: mock pool, get/release만 수백만 회 ─────────

#define BENCH_THREADS  24        // Ryzen 5600: 6코어 12스레드 → 2배
#define BENCH_ITER     1000000   // 스레드당 100만 회

typedef PGconn *(*get_conn_fn)(conn_pool *);

typedef struct {
    conn_pool   *pool;
    get_conn_fn  get_fn;
    long         ops;
} bench_arg_t;

void *bench_worker(void *arg)
{
    bench_arg_t *a = (bench_arg_t *)arg;
    int i;
    for (i = 0; i < BENCH_ITER; i++)
    {
        PGconn *conn = a->get_fn(a->pool);
        release_conn(a->pool, conn);
    }
    a->ops = BENCH_ITER;
    return NULL;
}

void bench_get_conn(const char *label, get_conn_fn get_fn)
{
    PGconn_mock mocks[CONN_SIZE];
    conn_pool *pool = make_mock_pool(mocks, CONN_SIZE);

    pthread_t   threads[BENCH_THREADS];
    bench_arg_t args[BENCH_THREADS];
    int i;

    struct timespec s, e;
    clock_gettime(CLOCK_MONOTONIC, &s);

    for (i = 0; i < BENCH_THREADS; i++)
    {
        args[i].pool   = pool;
        args[i].get_fn = get_fn;
        args[i].ops    = 0;
        pthread_create(&threads[i], NULL, bench_worker, &args[i]);
    }
    for (i = 0; i < BENCH_THREADS; i++)
        pthread_join(threads[i], NULL);

    clock_gettime(CLOCK_MONOTONIC, &e);

    long total_ops = (long)BENCH_THREADS * BENCH_ITER;
    double ms      = elapsed_ms(&s, &e);
    double mops    = total_ops / ms / 1000.0;   // Mops/s

    free_mock_pool(pool);
    printf("[BENCH] %-10s  %ld ops  %.2f ms  %.2f Mops/s\n",
           label, total_ops, ms, mops);
}

// ─── PG 공통 타입 ────────────────────────────────────────────

// ─── PG 실접속 풀 헬퍼 ───────────────────────────────────────

#define DB_HOST "host.docker.internal"
#define DB_PORT "5432"
#define DB_NAME "pgdb"
#define DB_USER "pguser"
#define DB_PASS "pgpass"
#define PG_CONNINFO "host=" DB_HOST " port=" DB_PORT " dbname=" DB_NAME " user=" DB_USER " password=" DB_PASS

static hash_map g_pg_map;
static wait_que g_pg_que;

static conn_pool *make_pg_pool(const char *conninfo, int n)
{
    int i;
    conn_pool *pool = calloc(1, sizeof(conn_pool));
    if (!pool)
        return NULL;

    strncpy(pool->connect_info, conninfo, sizeof(pool->connect_info) - 1);
    pool->map = &g_pg_map;
    pool->que = &g_pg_que;
    hash_init(pool->map);
    queue_init(pool->que);
    pthread_key_create(&pool->tls_key, NULL);

    for (i = 0; i < n; i++)
    {
        pool->conn_list[i] = PQconnectdb(conninfo);
        if (PQstatus(pool->conn_list[i]) != CONNECTION_OK)
        {
            fprintf(stderr, "[ERR] PQconnectdb[%d]: %s\n",
                    i, PQerrorMessage(pool->conn_list[i]));
            PQfinish(pool->conn_list[i]);
            pool->conn_list[i] = NULL;
        }
    }
    return pool;
}

static void free_pg_pool(conn_pool *pool)
{
    int i;
    for (i = 0; i < CONN_SIZE; i++)
        if (pool->conn_list[i])
            PQfinish(pool->conn_list[i]);
    pthread_key_delete(pool->tls_key);
    free(pool);
}

// ─── PG 단일스레드: 다양한 쿼리 타입 검증 ───────────────────

void test_pg_single(const char *label, get_conn_fn get_fn)
{
    int ok = 1;
    printf("[RUN] test_pg_single (%s)\n", label);

    conn_pool *pool = make_pg_pool(PG_CONNINFO, CONN_SIZE);
    if (!pool) { printf("[SKIP] test_pg_single: pool 생성 실패\n"); return; }

    RUN_TIMED(label, {
        PGconn *conn = get_fn(pool);
        if (!conn) { printf("[FAIL] get_conn NULL\n"); ok = 0; goto pg_single_done; }

        PGresult *res;

        // 1) 산술 연산 검증
        res = PQexec(conn, "SELECT 6 * 7");
        assert(PQresultStatus(res) == PGRES_TUPLES_OK);
        assert(strcmp(PQgetvalue(res, 0, 0), "42") == 0);
        PQclear(res);

        // 2) 임시 테이블 생성 → 100행 INSERT → COUNT 검증
        res = PQexec(conn, "CREATE TEMP TABLE _bench(id SERIAL, val INT)");
        assert(PQresultStatus(res) == PGRES_COMMAND_OK);
        PQclear(res);

        int i;
        for (i = 0; i < 100; i++)
        {
            char sql[64];
            snprintf(sql, sizeof(sql), "INSERT INTO _bench(val) VALUES(%d)", i);
            res = PQexec(conn, sql);
            if (PQresultStatus(res) != PGRES_COMMAND_OK)
            {
                fprintf(stderr, "[FAIL] INSERT %d: %s\n", i, PQerrorMessage(conn));
                ok = 0;
            }
            PQclear(res);
        }

        res = PQexec(conn, "SELECT COUNT(*) FROM _bench");
        assert(PQresultStatus(res) == PGRES_TUPLES_OK);
        assert(strcmp(PQgetvalue(res, 0, 0), "100") == 0);
        PQclear(res);

        // 3) SUM 검증 (0+1+...+99 = 4950)
        res = PQexec(conn, "SELECT SUM(val) FROM _bench");
        assert(PQresultStatus(res) == PGRES_TUPLES_OK);
        assert(strcmp(PQgetvalue(res, 0, 0), "4950") == 0);
        PQclear(res);

        // 4) DELETE → 빈 테이블 확인
        res = PQexec(conn, "DELETE FROM _bench");
        assert(PQresultStatus(res) == PGRES_COMMAND_OK);
        PQclear(res);

        res = PQexec(conn, "SELECT COUNT(*) FROM _bench");
        assert(strcmp(PQgetvalue(res, 0, 0), "0") == 0);
        PQclear(res);

        // 5) fast path 확인: 같은 스레드 재요청 시 같은 커넥션 반환
        release_conn(pool, conn);
        PGconn *c1 = get_fn(pool);
        release_conn(pool, c1);
        PGconn *c2 = get_fn(pool);
        assert(c1 == c2);
        release_conn(pool, c2);

        pg_single_done:;
    });

    free_pg_pool(pool);
    printf(ok ? "[PASS] test_pg_single (%s)\n" : "[FAIL] test_pg_single (%s)\n", label);
}

// ─── PG 멀티스레드: 풀 고갈 포함 동시 쿼리 ─────────────────

// 스레드 수를 CONN_SIZE 보다 크게 설정해 풀 고갈(대기 큐) 경로도 검증
#define PG_THREADS  20
#define PG_ITER     200

typedef struct {
    conn_pool   *pool;
    get_conn_fn  get_fn;
    int          thread_index;
    int          errors;
    int          queries;
} pg_worker_arg_t;

void *pg_worker(void *arg)
{
    pg_worker_arg_t *a = (pg_worker_arg_t *)arg;
    int i;
    for (i = 0; i < PG_ITER; i++)
    {
        PGconn *conn = a->get_fn(a->pool);
        if (!conn) { a->errors++; continue; }

        // 쿼리마다 결과값 검증
        char sql[64];
        snprintf(sql, sizeof(sql), "SELECT %d * 2", i);
        PGresult *res = PQexec(conn, sql);

        if (PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            fprintf(stderr, "[ERR] thread %d iter %d: %s\n",
                    a->thread_index, i, PQerrorMessage(conn));
            a->errors++;
        }
        else
        {
            int expected = i * 2;
            char exp_str[32];
            snprintf(exp_str, sizeof(exp_str), "%d", expected);
            if (strcmp(PQgetvalue(res, 0, 0), exp_str) != 0)
            {
                fprintf(stderr, "[ERR] thread %d: expected %s got %s\n",
                        a->thread_index, exp_str, PQgetvalue(res, 0, 0));
                a->errors++;
            }
            else
                a->queries++;
        }
        PQclear(res);
        release_conn(a->pool, conn);
    }
    return NULL;
}

void test_pg_multi(const char *label, get_conn_fn get_fn)
{
    printf("[RUN] test_pg_multi (%s, %d threads x %d iter, pool=%d)\n",
           label, PG_THREADS, PG_ITER, CONN_SIZE);

    conn_pool *pool = make_pg_pool(PG_CONNINFO, CONN_SIZE);
    if (!pool) { printf("[SKIP] test_pg_multi: pool 생성 실패\n"); return; }

    pthread_t       threads[PG_THREADS];
    pg_worker_arg_t args[PG_THREADS];
    int i, total_errors = 0, total_queries = 0;

    RUN_TIMED(label, {
        for (i = 0; i < PG_THREADS; i++)
        {
            args[i].pool         = pool;
            args[i].get_fn       = get_fn;
            args[i].thread_index = i + 1;
            args[i].errors       = 0;
            args[i].queries      = 0;
            pthread_create(&threads[i], NULL, pg_worker, &args[i]);
        }
        for (i = 0; i < PG_THREADS; i++)
            pthread_join(threads[i], NULL);
    });

    for (i = 0; i < PG_THREADS; i++)
    {
        total_errors  += args[i].errors;
        total_queries += args[i].queries;
    }

    free_pg_pool(pool);

    printf("  queries: %d / %d\n", total_queries, PG_THREADS * PG_ITER);
    if (total_errors == 0)
        printf("[PASS] test_pg_multi (%s)\n", label);
    else
        printf("[FAIL] test_pg_multi (%s): %d errors\n", label, total_errors);
}

// ─── PG 스트레스: BEGIN/COMMIT 트랜잭션 + 풀 반납 정확성 ─────

#define PG_STRESS_THREADS  30
#define PG_STRESS_ITER     100

typedef struct {
    conn_pool   *pool;
    get_conn_fn  get_fn;
    int          thread_index;
    int          errors;
} pg_stress_arg_t;

void *pg_stress_worker(void *arg)
{
    pg_stress_arg_t *a = (pg_stress_arg_t *)arg;
    int i;
    for (i = 0; i < PG_STRESS_ITER; i++)
    {
        PGconn *conn = a->get_fn(a->pool);
        if (!conn) { a->errors++; continue; }

        PGresult *res;

        res = PQexec(conn, "BEGIN");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) { a->errors++; PQclear(res); release_conn(a->pool, conn); continue; }
        PQclear(res);

        res = PQexec(conn, "SELECT txid_current()");
        if (PQresultStatus(res) != PGRES_TUPLES_OK) a->errors++;
        PQclear(res);

        res = PQexec(conn, "COMMIT");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) a->errors++;
        PQclear(res);

        release_conn(a->pool, conn);
    }
    return NULL;
}

void test_pg_stress(const char *label, get_conn_fn get_fn)
{
    printf("[RUN] test_pg_stress (%s, %d threads x %d iter, pool=%d)\n",
           label, PG_STRESS_THREADS, PG_STRESS_ITER, CONN_SIZE);

    conn_pool *pool = make_pg_pool(PG_CONNINFO, CONN_SIZE);
    if (!pool) { printf("[SKIP] test_pg_stress: pool 생성 실패\n"); return; }

    pthread_t       threads[PG_STRESS_THREADS];
    pg_stress_arg_t args[PG_STRESS_THREADS];
    int i, total_errors = 0;

    RUN_TIMED(label, {
        for (i = 0; i < PG_STRESS_THREADS; i++)
        {
            args[i].pool         = pool;
            args[i].get_fn       = get_fn;
            args[i].thread_index = i + 1;
            args[i].errors       = 0;
            pthread_create(&threads[i], NULL, pg_stress_worker, &args[i]);
        }
        for (i = 0; i < PG_STRESS_THREADS; i++)
            pthread_join(threads[i], NULL);
    });

    for (i = 0; i < PG_STRESS_THREADS; i++)
        total_errors += args[i].errors;

    free_pg_pool(pool);

    if (total_errors == 0)
        printf("[PASS] test_pg_stress (%s)\n", label);
    else
        printf("[FAIL] test_pg_stress (%s): %d errors\n", label, total_errors);
}

// ─── main ────────────────────────────────────────────────────

int main()
{
    test_single_thread();
    test_hash_collision();
    test_multi_thread();
    test_conn_single();
    test_conn_multi();

    printf("\n=== 마이크로벤치 (%d threads x %d iter) ===\n", BENCH_THREADS, BENCH_ITER);
    bench_get_conn("hash_map", get_conn);
    bench_get_conn("TLS",      get_conn_2);

    printf("\n=== PG 실접속 테스트 [hash_map] (%s) ===\n", PG_CONNINFO);
    test_pg_single("hash_map", get_conn);
    test_pg_multi ("hash_map", get_conn);
    test_pg_stress("hash_map", get_conn);

    printf("\n=== PG 실접속 테스트 [TLS] (%s) ===\n", PG_CONNINFO);
    test_pg_single("TLS", get_conn_2);
    test_pg_multi ("TLS", get_conn_2);
    test_pg_stress("TLS", get_conn_2);
    return 0;
}
