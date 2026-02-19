CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -I/usr/include/postgresql
LDFLAGS = -lpq -lpthread

TARGET  = housekeeper_test
SRCS    = main.c housekeeper.c

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS) $(LDFLAGS)

debug: CFLAGS += -g -O0 -fsanitize=thread
debug: LDFLAGS += -fsanitize=thread
debug: $(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all debug clean
