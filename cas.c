#include <stdio.h>

int main() {
    int value = 42;

    // value가 42면 100으로 바꿈
    if (__sync_bool_compare_and_swap(&value, 42, 100)) {
        printf("CAS success! value = %d\n", value);
    } else {
        printf("CAS failed. value = %d\n", value);
    }


    if (__sync_bool_compare_and_swap(&value, 42, 100)) {
        printf("CAS success! value = %d\n", value);
    } else {
        printf("CAS failed. value = %d\n", value);
    }
    return 0;
}
