#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <string.h>

#define NUM_THREADS 2
#define NUM_ITER 1000000

// Shared counter
volatile int counter = 0;

// For CAS lock
volatile int cas_lock = 0;

// For mutex lock
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

// CAS lock acquire
void cas_lock_acquire() {
    while (!__sync_bool_compare_and_swap(&cas_lock, 0, 1));
}

// CAS lock release
void cas_lock_release() {
    cas_lock = 0;
}

// Worker for CAS
void* cas_worker(void* arg) {
    for (int i = 0; i < NUM_ITER; ++i) {
        cas_lock_acquire();
        counter++;
        cas_lock_release();
    }
    return NULL;
}

// Worker for Mutex
void* mutex_worker(void* arg) {
    for (int i = 0; i < NUM_ITER; ++i) {
        pthread_mutex_lock(&mutex);
        counter++;
        pthread_mutex_unlock(&mutex);
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    if (argc != 2 || (strcmp(argv[1], "cas") != 0 && strcmp(argv[1], "mutex") != 0)) {
        printf("Usage: %s [cas|mutex]\n", argv[0]);
        return 1;
    }

    pthread_t threads[NUM_THREADS];
    void* (*worker)(void*) = strcmp(argv[1], "cas") == 0 ? cas_worker : mutex_worker;

    counter = 0;

    clock_t start = clock();

    for (int i = 0; i < NUM_THREADS; ++i)
        pthread_create(&threads[i], NULL, worker, NULL);

    for (int i = 0; i < NUM_THREADS; ++i)
        pthread_join(threads[i], NULL);

    clock_t end = clock();

    printf("Method: %s\n", argv[1]);
    printf("Final counter value: %d\n", counter);
    printf("Time taken: %.4f sec\n", (double)(end - start) / CLOCKS_PER_SEC);

    return 0;
}
