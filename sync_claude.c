#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <time.h>

#define NUM_CONNECTIONS 30

void check_conn_status(PGconn *conn, int index) {
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection %d failed: %s", 
                index, PQerrorMessage(conn));
        PQfinish(conn);
        exit(1);
    }
}

int main() {
    PGconn *conns[NUM_CONNECTIONS];
    PGresult *res;
    struct timespec ts;

    timespec_get(&ts, TIME_UTC); // 현재 시간(초, 나노초)를 구조체에 저장

    printf("현재 시간: %ld초 %ld나노초\n", ts.tv_sec, ts.tv_nsec);

    // 연결 정보
    const char *conninfo = "dbname=postgres user=postgres password=postgres host=127.0.0.1";
    
    // 10개 연결 생성
    printf("Creating %d connections...\n", NUM_CONNECTIONS);
    for(int i = 0; i < NUM_CONNECTIONS; i++) {
        conns[i] = PQconnectdb(conninfo);
        check_conn_status(conns[i], i);
        printf("Connection %d established\n", i);
    }
    timespec_get(&ts, TIME_UTC); // 현재 시간(초, 나노초)를 구조체에 저장

    printf("현재 시간: %ld초 %ld나노초\n", ts.tv_sec, ts.tv_nsec);
    // 각 연결에서 쿼리 실행
    for(int i = 0; i < NUM_CONNECTIONS; i++) {
        printf("\nExecuting query on connection %d\n", i);
        
        res = PQexec(conns[i], "SELECT current_timestamp");
        
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "Query failed on connection %d: %s", 
                    i, PQerrorMessage(conns[i]));
            PQclear(res);
            continue;
        }
        
        // 결과 출력
        if (PQntuples(res) > 0) {
            printf("Connection %d timestamp: %s\n", 
                   i, PQgetvalue(res, 0, 0));
        }
        
        PQclear(res);
    }
    
    // 연결 정리
    for(int i = 0; i < NUM_CONNECTIONS; i++) {
        PQfinish(conns[i]);
    }
    
    printf("\nAll connections closed\n");


    timespec_get(&ts, TIME_UTC); // 현재 시간(초, 나노초)를 구조체에 저장

    printf("현재 시간: %ld초 %ld나노초\n", ts.tv_sec, ts.tv_nsec);

    return 0;
}