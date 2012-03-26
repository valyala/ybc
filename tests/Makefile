YBC_FEATURE_FLAGS = -DYBC_HAVE_CLOCK_GETTIME -DYBC_HAVE_LINUX_FILE_API -DYBC_HAVE_LINUX_MMAN_API -DYBC_HAVE_PTHREAD -DYBC_HAVE_NANOSLEEP -DYBC_HAVE_LINUX_ERROR_API
GCC_FEATURE_FLAGS = -D_GNU_SOURCE -D_REENTRANT -D_THREAD_SAFE

CFLAGS = -Wall -Wextra -pedantic -std=c99 -pthread $(GCC_FEATURE_FLAGS) $(YBC_FEATURE_FLAGS)
LDFLAGS = -lrt

all: functional-32 functional-64

functional-32:  ../ybc.h ../ybc.c
	gcc ../ybc.c functional.c -I.. $(CFLAGS) $(LDFLAGS) -g -m32 -o functional-32

functional-64: ../ybc.h ../ybc.c
	gcc ../ybc.c functional.c -I.. $(CFLAGS) $(LDFLAGS) -g -m64 -o functional-64

clean:
	rm -f ./functional-32
	rm -f ./functional-64
