YBC_FEATURE_FLAGS = -DYBC_HAVE_CLOCK_GETTIME -DYBC_HAVE_LINUX_FILE_API -DYBC_HAVE_LINUX_MMAN_API -DYBC_HAVE_PTHREAD -DYBC_HAVE_NANOSLEEP -DYBC_HAVE_LINUX_ERROR_API
COMMON_FLAGS = -Wall -Wextra -pedantic -std=c99 -flto -D_GNU_SOURCE $(YBC_FEATURE_FLAGS)
SINGLE_THREADED_FLAGS = $(COMMON_FLAGS) -DYBC_SINGLE_THREADED
MULTI_THREADED_FLAGS = $(COMMON_FLAGS) -pthread -D_REENTRANT -D_THREAD_SAFE

RELEASE_FLAGS = -Os -DNDEBUG $(MULTI_THREADED_FLAGS)
DEBUG_FLAGS = -g $(MULTI_THREADED_FLAGS)
LIBYBC_FLAGS = -DYBC_BUILD_LIBRARY -shared -fpic -fwhole-program -lrt
TEST_FLAGS = -g $(MULTI_THREADED_FLAGS) -fwhole-program -lrt
SINGLE_THREADED_TEST_FLAGS = -g $(SINGLE_THREADED_FLAGS) -fwhole-program -lrt

YBC_SRCS = ybc.h ybc.c
TEST_SRCS = tests/functional.c

release: ybc-32-release ybc-64-release libybc-release

debug: ybc-32-debug ybc-64-debug libybc-debug

tests-release: tests-32-release tests-64-release tests-shared-release

tests-debug: tests-32-debug tests-64-debug tests-shared-debug

tests: tests-debug tests-release tests-single-threaded

all: release debug tests run-tests

ybc-32-release: $(YBC_SRCS)
	gcc -c ybc.c $(RELEASE_FLAGS) -m32 -o ybc-32-release.o

ybc-64-release: $(YBC_SRCS)
	gcc -c ybc.c $(RELEASE_FLAGS) -m64 -o ybc-64-release.o

ybc-32-debug: $(YBC_SRCS)
	gcc -c ybc.c $(DEBUG_FLAGS) -m32 -o ybc-32-debug.o

ybc-64-debug: $(YBC_SRCS)
	gcc -c ybc.c $(DEBUG_FLAGS) -m64 -o ybc-64-debug.o

libybc-debug: $(YBC_SRCS)
	gcc ybc.c $(DEBUG_FLAGS) $(LIBYBC_FLAGS) -o libybc-debug.so

libybc-release: $(YBC_SRCS)
	gcc ybc.c $(RELEASE_FLAGS) $(LIBYBC_FLAGS) -o libybc-release.so -m32

tests-32-release: ybc-32-release $(TEST_SRCS)
	gcc tests/functional.c ybc-32-release.o $(TEST_FLAGS) -m32 -o tests/functional-32-release

tests-64-release: ybc-64-release $(TEST_SRCS)
	gcc tests/functional.c ybc-64-release.o $(TEST_FLAGS) -m64 -o tests/functional-64-release

tests-32-debug: ybc-32-debug $(TEST_SRCS)
	gcc tests/functional.c ybc-32-debug.o $(TEST_FLAGS) -m32 -o tests/functional-32-debug

tests-64-debug: ybc-64-debug $(TEST_SRCS)
	gcc tests/functional.c ybc-64-debug.o $(TEST_FLAGS) -m64 -o tests/functional-64-debug

tests-shared-debug: libybc-debug $(TEST_SRCS)
	gcc tests/functional.c libybc-debug.so -Wl,-rpath,. $(TEST_FLAGS) -o tests/functional-shared-debug

tests-shared-release: libybc-release $(TEST_SRCS)
	gcc tests/functional.c libybc-release.so -Wl,-rpath,. $(TEST_FLAGS) -o tests/functional-shared-release

tests-single-threaded: $(YBC_SRCS) $(TEST_SRCS)
	gcc ybc.c tests/functional.c $(SINGLE_THREADED_TEST_FLAGS) -o tests/functional-single-threaded

run-tests: tests
	tests/functional-32-debug
	tests/functional-64-debug
	tests/functional-32-release
	tests/functional-64-release
	tests/functional-shared-debug
	tests/functional-shared-release
	tests/functional-single-threaded

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
	rm -f tests/functional-single-threaded
