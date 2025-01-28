#include <stdio.h>
#include <stdlib.h>
#include <poll.h>
#include <libpq-fe.h>
#include <string.h>

#define MAX_CONNS 3
#define POLL_TIMEOUT 1000  // 1초

typedef struct {
    PGconn *conn;
    int finished_connect;
    int query_sent;
    int query_finished;
    const char *conninfo;
} DBConn;

int handle_connection(DBConn *db) {
    PostgresPollingStatusType poll_status = PQconnectPoll(db->conn);
    
    switch (poll_status) 
    {
        case PGRES_POLLING_OK:
            printf("Connection established for %s\n", db->conninfo);
            db->finished_connect = 1;
            return 1;
        case PGRES_POLLING_FAILED:
            fprintf(stderr, "Connection failed for %s: %s\n", 
                    db->conninfo, PQerrorMessage(db->conn));
            return -1;
        default:
            return 0;
    }
}

void process_result(PGconn *conn) {
    PGresult *res;
    while ((res = PQgetResult(conn)) != NULL) 
    {
        if (PQresultStatus(res) == PGRES_TUPLES_OK) {
            int rows = PQntuples(res);
            for (int i = 0; i < rows; i++) {
                printf("Result from %s: %s\n", 
                       PQdb(conn), PQgetvalue(res, i, 0));
            }
        }
        PQclear(res);
    }
}

int main() {
    DBConn dbs[MAX_CONNS] = {
        {NULL, 0, 0, 0, "dbname=postgres user=postgres password=postgres host=127.0.0.1"},
        {NULL, 0, 0, 0, "dbname=postgres user=postgres password=postgres host=127.0.0.1"},
        {NULL, 0, 0, 0, "dbname=postgres user=postgres password=postgres host=127.0.0.1"}
    };
    
    struct pollfd fds[MAX_CONNS];
    
    // 비동기 연결 시작
    for (int i = 0; i < MAX_CONNS; i++) 
    {
        dbs[i].conn = PQconnectStart(dbs[i].conninfo);
        if (!dbs[i].conn) 
        {
            fprintf(stderr, "PQconnectStart failed for %s\n", dbs[i].conninfo);
            continue;
        }
        
        if (PQsetnonblocking(dbs[i].conn, 1) == -1) 
        {
            fprintf(stderr, "PQsetnonblocking failed for %s\n", dbs[i].conninfo);
            PQfinish(dbs[i].conn);
            dbs[i].conn = NULL;
            continue;
        }
        
        fds[i].fd = PQsocket(dbs[i].conn);
        fds[i].events = POLLIN | POLLOUT;
        fds[i].revents = 0;
    }
    
    while (1) 
    {
        // 모든 연결/쿼리가 완료되었는지 확인
        int all_done = 1;
        for (int i = 0; i < MAX_CONNS; i++) {
            if (!dbs[i].conn) continue;
            if (!dbs[i].query_finished) {
                all_done = 0;
                break;
            }
        }
        if (all_done) break;
        printf("polled!\n");
        // poll 호출
        int ready = poll(fds, MAX_CONNS, POLL_TIMEOUT);
        if (ready < 0) {
            perror("poll failed");
            break;
        }
        
        // 각 연결 처리
        for (int i = 0; i < MAX_CONNS; i++) 
        {
            if (!dbs[i].conn) continue;
            
            if (fds[i].revents & (POLLIN | POLLOUT)) 
            {
                // 연결 단계
                if (!dbs[i].finished_connect) 
                {
                    int status = handle_connection(&dbs[i]);
                    if (status < 0) 
                    {
                        PQfinish(dbs[i].conn);
                        dbs[i].conn = NULL;
                        continue;
                    }
                    // 연결이 완료되면 이벤트 변경
                    if (status == 1) 
                    {
                        fds[i].events = POLLIN;
                    }
                }
                // 쿼리 전송 단계
                else if (!dbs[i].query_sent) {
                    if (PQsendQuery(dbs[i].conn, "SELECT current_timestamp")) 
                    {
                        dbs[i].query_sent = 1;
                        fds[i].events = POLLIN;  // 응답 대기로 변경
                    }
                }
                // 결과 처리 단계
                else if (!dbs[i].query_finished && (fds[i].revents & POLLIN)) {
                    if (PQconsumeInput(dbs[i].conn)) 
                    {
                        if (!PQisBusy(dbs[i].conn)) 
                        {
                            process_result(dbs[i].conn);
                            dbs[i].query_finished = 1;
                        }
                    }
                }
            }
            
            // 에러 체크
            if (fds[i].revents & (POLLERR | POLLHUP | POLLNVAL)) {
                fprintf(stderr, "Poll error on connection %d\n", i);
                PQfinish(dbs[i].conn);
                dbs[i].conn = NULL;
                continue;
            }
        }
    }
    
    // 정리
    for (int i = 0; i < MAX_CONNS; i++) {
        if (dbs[i].conn) PQfinish(dbs[i].conn);
    }
    
    return 0;
}