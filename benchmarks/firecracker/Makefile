CC ?= gcc
CFLAGS ?= -O2 -Wall -Wextra
LDLIBS = -lm -pthread

all: bench_startup

bench_startup: bench_startup.c
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f bench_startup

.PHONY: all clean
