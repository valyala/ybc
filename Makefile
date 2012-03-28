YBC_FEATURE_FLAGS = -DYBC_HAVE_CLOCK_GETTIME -DYBC_HAVE_LINUX_FILE_API -DYBC_HAVE_LINUX_MMAN_API -DYBC_HAVE_PTHREAD -DYBC_HAVE_NANOSLEEP -DYBC_HAVE_LINUX_ERROR_API
COMMON_FLAGS = -Wall -Wextra -pedantic -std=c99 -flto -D_GNU_SOURCE $(YBC_FEATURE_FLAGS)
SINGLE_THREADED_FLAGS = $(COMMON_FLAGS) -DYBC_SINGLE_THREADED
MULTI_THREADED_FLAGS = $(COMMON_FLAGS) -pthread -D_REENTRANT -D_THREAD_SAFE

RELEASE_FLAGS = -O2 -DNDEBUG $(MULTI_THREADED_FLAGS)
DEBUG_FLAGS = -g $(MULTI_THREADED_FLAGS)
TEST_FLAGS = -g $(MULTI_THREADED_FLAGS) -lrt
SINGLE_THREADED_TEST_FLAGS = -g $(SINGLE_THREADED_FLAGS) -lrt

release: ybc-32-release ybc-64-release

debug: ybc-32-debug ybc-64-debug

tests-release: tests-32-release tests-64-release

tests-debug: tests-32-debug tests-64-debug

tests: tests-debug tests-release tests-single-threaded

all: release debug tests run-tests

ybc-32-release: ybc.h
	gcc -c ybc.c $(RELEASE_FLAGS) -m32 -o ybc-32-release.o

ybc-64-release: ybc.h
	gcc -c ybc.c $(RELEASE_FLAGS) -m64 -o ybc-64-release.o

ybc-32-debug: ybc.h
	gcc -c ybc.c $(DEBUG_FLAGS) -m32 -o ybc-32-debug.o

ybc-64-debug: ybc.h
	gcc -c ybc.c $(DEBUG_FLAGS) -m64 -o ybc-64-debug.o

tests-32-release: ybc-32-release
	gcc tests/functional.c ybc-32-release.o $(TEST_FLAGS) -m32 -o tests/functional-32-release

tests-64-release: ybc-64-release
	gcc tests/functional.c ybc-64-release.o $(TEST_FLAGS) -m64 -o tests/functional-64-release

tests-32-debug: ybc-32-debug
	gcc tests/functional.c ybc-32-debug.o $(TEST_FLAGS) -m32 -o tests/functional-32-debug

tests-64-debug: ybc-64-debug
	gcc tests/functional.c ybc-64-debug.o $(TEST_FLAGS) -m64 -o tests/functional-64-debug

tests-single-threaded: ybc.h
	gcc ybc.c tests/functional.c $(SINGLE_THREADED_TEST_FLAGS) -o tests/functional-single-threaded

run-tests: tests
	tests/functional-32-debug
	tests/functional-64-debug
	tests/functional-32-release
	tests/functional-64-release
	tests/functional-single-threaded

clean:
	rm -f ybc-32-release.o
	rm -f ybc-64-release.o
	rm -f ybc-32-debug.o
	rm -f ybc-64-debug.o
	rm -f tests/functional-32-release
	rm -f tests/functional-64-release
	rm -f tests/functional-32-debug
	rm -f tests/functional-64-debug
	rm -f tests/functional-single-threaded
