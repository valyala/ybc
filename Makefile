CC = gcc

YBC_FEATURE_FLAGS = -DYBC_HAVE_CLOCK_GETTIME -DYBC_HAVE_LINUX_FILE_API -DYBC_HAVE_LINUX_MMAN_API -DYBC_HAVE_PTHREAD -DYBC_HAVE_NANOSLEEP -DYBC_HAVE_LINUX_ERROR_API

COMMON_FLAGS = -Wall -Wextra -Werror -pedantic -std=c99 -pthread -flto -D_GNU_SOURCE -D_REENTRANT -D_THREAD_SAFE $(YBC_FEATURE_FLAGS)
MULTI_THREADED_FLAGS = $(COMMON_FLAGS)

RELEASE_FLAGS = -O2 -DNDEBUG $(MULTI_THREADED_FLAGS)
DEBUG_FLAGS = -g $(MULTI_THREADED_FLAGS)
LIBYBC_FLAGS = -DYBC_BUILD_LIBRARY -shared -fpic -fwhole-program -lrt
TEST_FLAGS = -g $(MULTI_THREADED_FLAGS) -fwhole-program -lrt
PERFTEST_FLAGS = $(MULTI_THREADED_FLAGS) -fwhole-program -lrt

VALGRIND_FLAGS = --suppressions=valgrind.supp --track-fds=yes

YBC_SRCS = ybc.c
TEST_SRCS = tests/functional.c
PERFTEST_SRCS = tests/performance.c

release: ybc-32-release ybc-64-release libybc-release

debug: ybc-32-debug ybc-64-debug libybc-debug

tests-release: tests-32-release tests-64-release tests-shared-release

tests-debug: tests-32-debug tests-64-debug tests-shared-debug

tests: tests-debug tests-release

perftests-release: perftests-32-release perftests-64-release

perftests-debug: perftests-32-debug perftests-64-debug

perftests: perftests-debug perftests-release

all: release debug tests perftests run-tests run-valgrind-tests run-perftests

ybc.c: ybc.h

tests/functional.c: ybc.h

tests/performance.c: ybc.h

ybc-32-release: $(YBC_SRCS)
	$(CC) -c $(YBC_SRCS) $(RELEASE_FLAGS) -m32 -o ybc-32-release.o

ybc-64-release: $(YBC_SRCS)
	$(CC) -c $(YBC_SRCS) $(RELEASE_FLAGS) -m64 -o ybc-64-release.o

ybc-32-debug: $(YBC_SRCS)
	$(CC) -c $(YBC_SRCS) $(DEBUG_FLAGS) -m32 -o ybc-32-debug.o

ybc-64-debug: $(YBC_SRCS)
	$(CC) -c $(YBC_SRCS) $(DEBUG_FLAGS) -m64 -o ybc-64-debug.o

libybc-debug: $(YBC_SRCS)
	$(CC) $(YBC_SRCS) $(DEBUG_FLAGS) $(LIBYBC_FLAGS) -o libybc-debug.so

libybc-release: $(YBC_SRCS)
	$(CC) $(YBC_SRCS) $(RELEASE_FLAGS) $(LIBYBC_FLAGS) -o libybc-release.so

tests-32-release: ybc-32-release $(TEST_SRCS)
	$(CC) $(TEST_SRCS) ybc-32-release.o $(TEST_FLAGS) -m32 -o tests/functional-32-release

tests-64-release: ybc-64-release $(TEST_SRCS)
	$(CC) $(TEST_SRCS) ybc-64-release.o $(TEST_FLAGS) -m64 -o tests/functional-64-release

tests-32-debug: ybc-32-debug $(TEST_SRCS)
	$(CC) $(TEST_SRCS) ybc-32-debug.o $(TEST_FLAGS) -m32 -o tests/functional-32-debug

tests-64-debug: ybc-64-debug $(TEST_SRCS)
	$(CC) $(TEST_SRCS) ybc-64-debug.o $(TEST_FLAGS) -m64 -o tests/functional-64-debug

tests-shared-debug: libybc-debug $(TEST_SRCS)
	$(CC) $(TEST_SRCS) libybc-debug.so -Wl,-rpath,. $(TEST_FLAGS) -o tests/functional-shared-debug

tests-shared-release: libybc-release $(TEST_SRCS)
	$(CC) $(TEST_SRCS) libybc-release.so -Wl,-rpath,. $(TEST_FLAGS) -o tests/functional-shared-release

perftests-32-release: ybc-32-release $(PERFTEST_SRCS)
	$(CC) $(PERFTEST_SRCS) ybc-32-release.o $(PERFTEST_FLAGS) -O2 -DNDEBUG -m32 -o tests/performance-32-release

perftests-64-release: ybc-64-release $(PERFTEST_SRCS)
	$(CC) $(PERFTEST_SRCS) ybc-64-release.o $(PERFTEST_FLAGS) -O2 -DNDEBUG -m64 -o tests/performance-64-release

perftests-32-debug: ybc-32-debug $(PERFTEST_SRCS)
	$(CC) $(PERFTEST_SRCS) ybc-32-debug.o $(PERFTEST_FLAGS) -g -m32 -o tests/performance-32-debug

perftests-64-debug: ybc-64-debug $(PERFTEST_SRCS)
	$(CC) $(PERFTEST_SRCS) ybc-64-debug.o $(PERFTEST_FLAGS) -g -m64 -o tests/performance-64-debug

run-tests: tests
	tests/functional-32-debug
	tests/functional-64-debug
	tests/functional-32-release
	tests/functional-64-release
	tests/functional-shared-debug
	tests/functional-shared-release

run-valgrind-tests: tests-shared-debug
	valgrind $(VALGRIND_FLAGS) tests/functional-shared-debug
	valgrind $(VALGRIND_FLAGS) tests/functional-shared-release

run-perftests: perftests
	tests/performance-32-debug
	tests/performance-64-debug
	tests/performance-32-release
	tests/performance-64-release

clean:
	rm -f ybc-32-release.o
	rm -f ybc-64-release.o
	rm -f ybc-32-debug.o
	rm -f ybc-64-debug.o
	rm -f libybc-release.so
	rm -f libybc-debug.so
	rm -f tests/functional-32-release
	rm -f tests/functional-64-release
	rm -f tests/functional-32-debug
	rm -f tests/functional-64-debug
	rm -f tests/functional-shared-release
	rm -f tests/functional-shared-debug
	rm -f tests/performance-32-release
	rm -f tests/performance-64-release
	rm -f tests/performance-32-debug
	rm -f tests/performance-64-debug
