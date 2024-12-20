#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<sys/socket.h>
#include<pthread.h>
#include <semaphore.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/time.h>
#define mq_key 2024

#define MAX 20
#define MIN 4

static int running = 0;
static int waiting = 0;
static int thread_id[MAX] = {0};
static long end_time[MAX] = {0};

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;


struct Node
{
    struct Node* next;
    int data;
};
struct Queue
{
    int node_cnt;
    struct Node* head;
    struct Node* tail;
};
static struct Queue* client_que;
static struct Queue* thread_que;

int queue_len(struct Queue* que);
struct Queue* queue_init();
void append(struct Queue* que, int data);
int popleft(struct Queue* que);
void free_queue(struct  Queue* que);
void error_handling(char *message);
void* get_message(void* args);
void* gc(void* args);

int main(int argc, char *argv[]){
	int serv_sock;
	int clnt_sock;
    int str_len;
	int pthread_id;
	int input[2];
	char message1[30] = "max connection";
	char message2[30] = "plez wait";
	
    client_que = queue_init();
	thread_que = queue_init();

    pthread_t gc;                                                                       //msg_id 2l로 thread index 받기
	pthread_t pthread_list[MAX + 1];
	for(int i =0; i < MAX; i++){
		thread_id[i] = i;
	}												    //thread MAX개 생성 준비
	for (int i = 0; i < MIN; i++){
		pthread_create(&pthread_list[i], NULL, get_message_thread, (void*)thread_id + i);             // 최소 유지되는 쓰레드 생성
        waiting++;
	}
	for (int i = MIN; i < MAX + 1; i++){                                                //생성할 수 있는 쓰레드 index 큐에 넣기
		append(thread_que, i);
	}
	struct sockaddr_in serv_addr;
	struct sockaddr_in clnt_addr;
	socklen_t clnt_addr_size = sizeof(clnt_addr);
	serv_sock=socket(PF_INET, SOCK_STREAM, 0); 											// 소켓 생성(이후 bind와 accept를 호출하기에 서버소켓이 된다.)
	if(serv_sock==-1){
		error_handling("socket() error");
	}
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family=AF_INET;
	serv_addr.sin_addr.s_addr=htonl(INADDR_ANY);
	serv_addr.sin_port=htons(atoi("1234"));
    int true = 1;
	setsockopt(serv_sock, SOL_SOCKET, SO_REUSEADDR, &true, sizeof(true));

	if(bind(serv_sock, (struct sockaddr*) &serv_addr, sizeof(serv_addr))==-1){ 			// IP주소, PORT번호 할당
		error_handling("bind() error");
		return 0;
	}
	if(listen(serv_sock, 20)==-1){ 														// 소켓 연결요청 받아들일 수 있는 상태가 됨 
		error_handling("listen() error");
		return 0;
	}

	printf("server is listening!\n", clnt_sock);	
	while(1) {
	    clnt_sock=accept(serv_sock, (struct sockaddr*)&clnt_addr, &clnt_addr_size); 	// 연결요청이 있을 때 까지 함수는 반환되지 않음
        pthread_mutex_lock(&mutex);
		if(clnt_sock == -1){
	    	error_handling("accept() error");
			pthread_mutex_unlock(&mutex);
			continue;
		}
        if(running == MAX){
			close(clnt_sock);
		}else{
            if (running == waiting++){                                                     //대기중인 쓰레드 없으면 생성
				pthread_id = popoleft(thread_que);
                pthread_create(&pthread_list[pthread_id], NULL, get_message_thread, (void*)thread_id + pthread_id);
            }
			running++;
			append(client_que, clnt_sock);												//clnt_sock 큐에 넣기
			pthread_cond_signal(&cond);
		}
		pthread_mutex_unlock(&mutex);
    }
	printf("server close\n");
	close(serv_sock);
	pthread_mutex_destroy(&mutex);
	return 0;
}
void error_handling(char *message){
	fputs(message, stderr);
	fputc('\n', stderr);
}
void* get_message_thread(void* args){
	int pthread_id = *(int*) args;
    int str_len;
    int temp;
	int clnt_sock;  																	// client_sock 정보 받기
	while(1){
    	pthread_mutex_lock(&mutex);
		pthread_cond_wait(&cond, &mutex);
		clnt_sock = popleft(client_que);												//클라이언트 큐에서 소켓 id pop
        if (clnt_sock == 0){                                                    		//gc종료 스레드 큐에 스레드 index 넣기
		    close(clnt_sock);
			append(thread_que, pthread_id);
    	    printf("socket id %d closed \n", clnt_sock);
            end_time[running--] = (long)time(NULL);
            pthread_mutex_unlock(&mutex);
            continue;
        }
		pthread_mutex_unlock(&mutex);

    	printf("socket id: %d thread id: %lu\n", clnt_sock, pthread_self());
		char message[30];													
		while(1){
		    str_len=read(clnt_sock, message, sizeof(message)-1);
		    if(str_len==-1) {error_handling("read() error"); break;}
			str_len = send(clnt_sock, message, sizeof(message)-1, MSG_DONTWAIT);
    	    if(str_len == 0) error_handling("send error");
    	    printf("socket id %d:", clnt_sock);
    	    printf("%s\n", message);
    	}
		pthread_mutex_lock(&mutex);
		close(clnt_sock);
    	printf("socket id %d closed \n", clnt_sock);                            		//쓰레드 대기
		append(thread_que, thread_id); 
        end_time[running--] = (long)time(NULL);                                            	//종료시간 현재 시간으로 변경
		pthread_mutex_unlock(&mutex);
	}
	return NULL;
}
void* gc(void* args){
	while(1){
		pthread_mutex_lock(&mutex);
    	while (1){
    	    if(waiting == MIN || waiting == running) break;
    	    if((long)time(NULL) - end_time[waiting--] > 60){                                   // 현재 시간 받아서 비교
				append(client_que, 0);
    	    }else{
    	        break;
    	    }
    	}
		pthread_mutex_unlock(&mutex);
		sleep(10);    
	}
}

int queue_len(struct Queue* que){
    return que->node_cnt;
}
struct Queue* queue_init(){
    struct Queue* new_queue = malloc(sizeof(struct Queue));
    new_queue->head = NULL;
    new_queue->tail = NULL;
    new_queue->node_cnt = 0;
    return new_queue;
};
void append(struct Queue* que, int data){
    struct Node* new_node = malloc(sizeof(struct Node));
    new_node->data = data;
    if (que->head == NULL){
        que->head = new_node;
        que->tail = new_node;
        que->node_cnt = 1;
    }else{
        que->tail->next = new_node;
        que->tail = new_node;
        que->node_cnt++;
    }
}
int popleft(struct Queue* que){
    if (que->head == NULL)
        return -1;
    struct Node* poped_node = que->head;
    que->head = poped_node->next;
    que->node_cnt--;
    int res = poped_node->data;
    free(poped_node);
    return res;
}
void free_queue(struct  Queue* que){
    while(que->node_cnt > 0){
        struct Node* poped_node = que->head;
        que->head = poped_node->next;
        que->node_cnt--;
        free(poped_node);        
    }
    free(que);   
};
