#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <poll.h>
#include <libpq-fe.h>

#define TIMEOUT 5000  // 5초 (밀리초 단위)

int main() {
    struct pollfd fds[2];
    int ret, i;
    PostgresPollingStatusType status;

    const char *conninfo = "dbname=postgres user=postgres password=postgres host=127.0.0.1";
    PGconn *conn[2];

    PGconn *conn2;
    PGresult *res;
    {

        // 데이터베이스 연결
        conn2 = PQconnectdb(conninfo);

        if (PQstatus(conn2) != CONNECTION_OK) {
            fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn2));
            PQfinish(conn2);
            exit(1);
        }

        // SQL 쿼리 실행
        res = PQexec(conn2, "SELECT 1");

        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            fprintf(stderr, "Query failed: %s\n", PQerrorMessage(conn2));
            PQclear(res);
            PQfinish(conn2);
            exit(1);
        }

        // 결과 출력
        int nFields = PQnfields(res);
        for (int i = 0; i < PQntuples(res); i++) {
            for (int j = 0; j < nFields; j++) {
                printf("%s ", PQgetvalue(res, i, j));
            }
            printf("\n");
        }

        // 메모리 해제
        PQclear(res);
        PQfinish(conn2);

    }

    // 비동기 연결 시작
    conn[0] = PQconnectStart(conninfo);
    conn[1] = PQconnectStart(conninfo);

    // pollfd 구조체 설정
    fds[0].fd = PQsocket(conn[0]);
    fds[0].events = POLLIN;
    fds[1].fd = PQsocket(conn[1]);
    fds[1].events = POLLIN;

    // 연결 확인 및 처리
    while (1) { // 모든 연결이 완료될 때까지 반복
        ret = poll(fds, 2, TIMEOUT);
        printf("poll! %d\n", ret);
        if (ret < 0) {
            perror("poll failed");
            break; // 오류 발생 시 루프 종료
        }

        for (i = 0; i < 2; i++) {
            if (ret > 0 && (fds[i].revents & POLLIN)) {
                status = PQconnectPoll(conn[i]);

                if (status == PGRES_POLLING_OK) {
                    printf("Connection %d established!\n", i);
                    // 연결 성공 시 처리 (쿼리 실행 등)
                } else if (status == PGRES_POLLING_FAILED) {
                    fprintf(stderr, "Connection %d failed: %s\n", i, PQerrorMessage(conn[i]));
                    PQfinish(conn[i]); // 연결 종료
                }
            }
        }

        // 모든 연결이 성공했는지 확인
        if (PQstatus(conn[0]) == CONNECTION_OK && PQstatus(conn[1]) == CONNECTION_OK) {
            break;
        }
    }

    // 연결 종료
    for (i = 0; i < 2; i++) {
        PQfinish(conn[i]);
    }

    return 0;
}