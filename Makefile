CC = gcc
GOCC = GOMAXPROCS=4 go

PWD = `pwd`

COMMON_FLAGS = -Wall -Wextra -Werror -pedantic -std=c99 -pthread -flto -DYBC_PLATFORM_LINUX

RELEASE_FLAGS = -O2 -DNDEBUG $(COMMON_FLAGS)
DEBUG_FLAGS = -g $(COMMON_FLAGS)
LIBYBC_FLAGS = -DYBC_BUILD_LIBRARY -shared -fpic -fwhole-program -lrt
TEST_FLAGS = -g $(COMMON_FLAGS) -fwhole-program -lrt -Wno-unused-function
PERFTEST_FLAGS = $(COMMON_FLAGS) -fwhole-program -lrt -Wno-unused-function

VALGRIND_FLAGS = --suppressions=valgrind.supp --track-fds=yes

YBC_SRCS = ybc.c
TEST_SRCS = tests/functional.c
PERFTEST_SRCS = tests/performance.c

release: ybc-32-release ybc-64-release libybc-release

debug: ybc-32-debug ybc-64-debug libybc-debug

build-tests-release: build-tests-32-release build-tests-64-release build-tests-shared-release

build-tests-debug: build-tests-32-debug build-tests-64-debug build-tests-shared-debug

build-tests: build-tests-debug build-tests-release

build-perftests-release: build-perftests-32-release build-perftests-64-release

build-perftests-debug: build-perftests-32-debug build-perftests-64-debug

build-perftests: build-perftests-debug build-perftests-release

all: release debug tests valgrind-tests perftests

ybc.c: ybc.h platform.h platform/linux.c

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

build-tests-32-release: ybc-32-release $(TEST_SRCS)
	$(CC) $(TEST_SRCS) ybc-32-release.o $(TEST_FLAGS) -m32 -o tests/functional-32-release

build-tests-64-release: ybc-64-release $(TEST_SRCS)
	$(CC) $(TEST_SRCS) ybc-64-release.o $(TEST_FLAGS) -m64 -o tests/functional-64-release

build-tests-32-debug: ybc-32-debug $(TEST_SRCS)
	$(CC) $(TEST_SRCS) ybc-32-debug.o $(TEST_FLAGS) -m32 -o tests/functional-32-debug

build-tests-64-debug: ybc-64-debug $(TEST_SRCS)
	$(CC) $(TEST_SRCS) ybc-64-debug.o $(TEST_FLAGS) -m64 -o tests/functional-64-debug

build-tests-shared-debug: libybc-debug $(TEST_SRCS)
	$(CC) $(TEST_SRCS) -L. -lybc-debug -Wl,-rpath,. $(TEST_FLAGS) -o tests/functional-shared-debug

build-tests-shared-release: libybc-release $(TEST_SRCS)
	$(CC) $(TEST_SRCS) -L. -lybc-release -Wl,-rpath,. $(TEST_FLAGS) -o tests/functional-shared-release

build-perftests-32-release: ybc-32-release $(PERFTEST_SRCS)
	$(CC) $(PERFTEST_SRCS) ybc-32-release.o $(PERFTEST_FLAGS) -O2 -DNDEBUG -m32 -o tests/performance-32-release

build-perftests-64-release: ybc-64-release $(PERFTEST_SRCS)
	$(CC) $(PERFTEST_SRCS) ybc-64-release.o $(PERFTEST_FLAGS) -O2 -DNDEBUG -m64 -o tests/performance-64-release

build-perftests-32-debug: ybc-32-debug $(PERFTEST_SRCS)
	$(CC) $(PERFTEST_SRCS) ybc-32-debug.o $(PERFTEST_FLAGS) -g -m32 -o tests/performance-32-debug

build-perftests-64-debug: ybc-64-debug $(PERFTEST_SRCS)
	$(CC) $(PERFTEST_SRCS) ybc-64-debug.o $(PERFTEST_FLAGS) -g -m64 -o tests/performance-64-debug

tests: build-tests
	tests/functional-32-debug
	tests/functional-64-debug
	tests/functional-32-release
	tests/functional-64-release
	tests/functional-shared-debug
	tests/functional-shared-release

go-tests-debug:
	$(GOCC) test -a ./bindings/go/ybc
	$(GOCC) test -a ./libs/go/memcache

go-tests-release:
	$(GOCC) test -a -tags release ./bindings/go/ybc
	$(GOCC) test -a -tags release ./libs/go/memcache

go-tests: go-tests-debug go-tests-release

valgrind-tests: build-tests-shared-debug build-tests-shared-release
	valgrind $(VALGRIND_FLAGS) tests/functional-shared-debug
	valgrind $(VALGRIND_FLAGS) tests/functional-shared-release

perftests: build-perftests
	tests/performance-32-debug
	tests/performance-64-debug
	tests/performance-32-release
	tests/performance-64-release

go-perftests-bindings:
	$(GOCC) test -a -tags release -test.bench=".*" ./bindings/go/ybc

go-perftests-memcache:
	$(GOCC) test -a -tags release -test.bench=".*" ./libs/go/memcache

go-perftests: go-perftests-bindings go-perftests-memcache

go-cdn-booster:
	$(GOCC) build -o go-cdn-booster -a -tags release ./apps/go/cdn-booster

go-cdn-booster-bench:
	$(GOCC) build -o go-cdn-booster-bench -a -tags release ./apps/go/cdn-booster-bench

go-memcached:
	$(GOCC) build -o go-memcached -a -tags release ./apps/go/memcached

go-memcached-bench:
	$(GOCC) build -o go-memcached-bench -a -tags release ./apps/go/memcached-bench

go-update:
	$(GOCC) get -u github.com/vharitonsky/iniflags
	$(GOCC) get -u github.com/valyala/ybc/bindings/go/ybc
	$(GOCC) get -u github.com/valyala/ybc/libs/go/memcache
	$(GOCC) get -u github.com/valyala/ybc/apps/go/cdn-booster
	$(GOCC) get -u github.com/valyala/ybc/apps/go/cdn-booster-bench
	$(GOCC) get -u github.com/valyala/ybc/apps/go/memcached
	$(GOCC) get -u github.com/valyala/ybc/apps/go/memcached-bench

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
	rm -f go-cdn-booster
	rm -f go-cdn-booster-bench
	rm -f go-memcached
	rm -f go-memcached-bench
