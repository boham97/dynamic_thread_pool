#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

void exit_nicely(PGconn *conn) {
    PQfinish(conn);
    exit(1);
}

int main() {
    const char *conninfo = "dbname=postgres user=postgres password=postgres host=127.0.0.1";
    PGconn *conn;
    PGresult *res;

    // 데이터베이스 연결
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed: %s", PQerrorMessage(conn));
        exit_nicely(conn);
    }

    // 비동기 쿼리 전송
    int send_query_result = PQsendQuery(conn, "SELECT 1");
    if (!send_query_result) {
        fprintf(stderr, "Failed to send query: %s", PQerrorMessage(conn));
        exit_nicely(conn);
    }

    // 비동기 처리를 위해 싱글 row 모드 설정
    PQsetSingleRowMode(conn);

    // 결과 처리
    while ((res = PQgetResult(conn)) != NULL) {
        // 쿼리 상태 확인
        if (PQresultStatus(res) == PGRES_SINGLE_TUPLE) {
            int rows = PQntuples(res);
            int cols = PQnfields(res);

            // 각 row 처리
            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    printf("%s\t", PQgetvalue(res, i, j));
                }
                printf("\n");
            }
        } else if (PQresultStatus(res) == PGRES_TUPLES_OK) {
            // 모든 row 처리 완료
            printf("Query completed successfully\n");
        } else {
            fprintf(stderr, "Query failed: %s", PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }
        PQclear(res);
    }

    // 연결 종료
    PQfinish(conn);
    return 0;
}