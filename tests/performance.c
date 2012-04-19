/*
 * Performance tests.
 */

#include "../ybc.h"

#include <assert.h>
#include <stdint.h>  /* uint*_t */
#include <stdio.h>   /* printf */
#include <stdlib.h>  /* exit, malloc, free */
#include <string.h>  /* memset, memcmp */


#define M_ERROR(error_message)  do { \
  fprintf(stderr, "%s\n", error_message); \
  exit(EXIT_FAILURE); \
} while (0)


#ifdef YBC_HAVE_CLOCK_GETTIME

#include <time.h>  /* clock_gettime, timespec */

static double m_get_current_time(void)
{
  struct timespec t;

  const int rv = clock_gettime(CLOCK_MONOTONIC, &t);
  assert(rv == 0);
  (void)rv;

  return t.tv_sec + t.tv_nsec / (1000.0 * 1000 * 1000);
}

#else  /* !YBC_HAVE_CLOCK_GETTIME */
#error "Unsupported time implementation"
#endif


#ifdef YBC_HAVE_PTHREAD

#include <pthread.h>
#include <sys/types.h>  /* pthread_*_t */

struct thread
{
  pthread_t t;
};

static void start_thread(struct thread *const t, void *(*func)(void *),
    void *const ctx)
{
  if (pthread_create(&t->t, NULL, func, ctx) != 0) {
    M_ERROR("Cannot create new thread");
  }
}

static void *join_thread(struct thread *const t)
{
  void *retval;
  if (pthread_join(t->t, &retval) != 0) {
    M_ERROR("Cannot join thread");
  }
  return retval;
}

struct m_lock
{
  pthread_mutex_t mutex;
};

static void m_lock_init(struct m_lock *const lock)
{
  if (pthread_mutex_init(&lock->mutex, NULL) != 0) {
    M_ERROR("pthread_mutex_init()");
  }
}

static void m_lock_destroy(struct m_lock *const lock)
{
  const int rv = pthread_mutex_destroy(&lock->mutex);
  assert(rv == 0);
  (void)rv;
}

static void m_lock_lock(struct m_lock *const lock)
{
  const int rv = pthread_mutex_lock(&lock->mutex);
  assert(rv == 0);
  (void)rv;
}

static void m_lock_unlock(struct m_lock *const lock)
{
  const int rv = pthread_mutex_unlock(&lock->mutex);
  assert(rv == 0);
  (void)rv;
}

#else  /* !YBC_HAVE_PTHREAD */
#error "Unsupported thread implementation"
#endif


struct m_rand_state
{
  uint64_t s;
};

static void m_rand_init(struct m_rand_state *const rs)
{
  rs->s = m_get_current_time() * 1000;
}

static uint64_t m_rand_next(struct m_rand_state *const rs)
{
  /*
   * These magic numbers are stolen from the table 'Parameters in common use'
   * at http://en.wikipedia.org/wiki/Linear_congruential_generator .
   */
  static const uint64_t a = 6364136223846793005;
  static const uint64_t c = 1442695040888963407;

  rs->s = rs->s * a + c;
  return rs->s;
}

/*
 * Use custom memset() implementation in order to avoid silly
 * "memset used with constant zero length parameter" warning in gcc.
 * See https://bugzilla.redhat.com/show_bug.cgi?id=452219 .
 */
static void m_memset(void *const s, const char c, const size_t n)
{
  char *const ss = s;

  for (size_t i = 0; i < n; ++i) {
    ss[i] = c;
  }
}

static int m_memset_check(const void *const s, const char c, const size_t n)
{
  const char *const ss = s;

  for (size_t i = 0; i < n; ++i) {
    if (ss[i] != c) {
      return 0;
    }
  }
  return 1;
}

static void simple_add(struct ybc *const cache, const size_t requests_count,
    const size_t items_count, const size_t max_item_size)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;
  struct m_rand_state rand_state;
  uint64_t tmp;

  char *const buf = malloc(max_item_size);

  if (buf == NULL) {
    M_ERROR("Cannot allocate memory");
  }

  const struct ybc_key key = {
      .ptr = &tmp,
      .size = sizeof(tmp),
  };
  struct ybc_value value = {
      .ptr = buf,
      .size = 0,
      .ttl = YBC_MAX_TTL,
  };

  m_rand_init(&rand_state);

  for (size_t i = 0; i < requests_count; ++i) {
    tmp = m_rand_next(&rand_state) % items_count;
    value.size = m_rand_next(&rand_state) % (max_item_size + 1);
    m_memset(buf, (char)value.size, value.size);

    if (!ybc_item_add(cache, item, &key, &value)) {
      M_ERROR("Cannot add item to the cache");
    }
    ybc_item_release(item);
  }

  free(buf);
}

static void simple_get(struct ybc *const cache, const size_t requests_count,
    const size_t items_count, const size_t max_item_size)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;
  struct m_rand_state rand_state;
  uint64_t tmp;

  const struct ybc_key key = {
      .ptr = &tmp,
      .size = sizeof(tmp),
  };
  struct ybc_value value;

  m_rand_init(&rand_state);

  for (size_t i = 0; i < requests_count; ++i) {
    tmp = m_rand_next(&rand_state) % items_count;

    if (ybc_item_acquire(cache, item, &key)) {
      /* Emulate access to the item */
      ybc_item_get_value(item, &value);
      if (value.size > max_item_size) {
        M_ERROR("Unexpected value size");
      }
      if (!m_memset_check(value.ptr, (char)value.size, value.size)) {
        M_ERROR("Unexpected value");
      }
      ybc_item_release(item);
    }
  }
}

static void m_open(struct ybc *const cache, const size_t items_count,
    const size_t hot_items_count, const size_t max_item_size)
{
  char config_buf[ybc_config_get_size()];
  struct ybc_config *const config = (struct ybc_config *)config_buf;

  const size_t data_file_size = max_item_size * items_count;
  const size_t hot_data_size = max_item_size * hot_items_count;

  ybc_config_init(config);
  ybc_config_set_max_items_count(config, items_count);
  ybc_config_set_hot_items_count(config, hot_items_count);
  ybc_config_set_data_file_size(config, data_file_size);
  ybc_config_set_hot_data_size(config, hot_data_size);

  ybc_config_set_sync_interval(config, 200);

  if (!ybc_open(cache, config, 1)) {
    M_ERROR("Cannot create a cache");
  }

  ybc_config_destroy(config);
}

static void measure_simple_ops(struct ybc *const cache,
    const size_t requests_count, const size_t items_count,
    const size_t hot_items_count, const size_t max_item_size)
{
  double start_time, end_time;
  double qps;

  m_open(cache, items_count, hot_items_count, max_item_size);

  printf("simple_ops(requests_count=%zu, items_count=%zu, "
      "hot_items_count=%zu, max_item_size=%zu)\n",
      requests_count, items_count, hot_items_count, max_item_size);

  start_time = m_get_current_time();
  const double first_start_time = start_time;
  simple_add(cache, requests_count, items_count, max_item_size);
  end_time = m_get_current_time();

  qps = requests_count / (end_time - start_time);
  printf("  simple_add: %.02f qps\n", qps);

  const size_t get_items_count = hot_items_count ? hot_items_count :
      items_count;
  start_time = m_get_current_time();
  simple_get(cache, requests_count, get_items_count, max_item_size);
  end_time = m_get_current_time();

  qps = requests_count / (end_time - start_time);
  printf("  simple_get: %.02f qps\n", qps);

  qps = 2 * requests_count / (end_time - first_start_time);
  printf("  avg %.02f qps\n", qps);

  ybc_close(cache);
}

struct thread_task
{
  struct m_lock lock;
  struct ybc *cache;
  size_t requests_count;
  size_t items_count;
  size_t get_items_count;
  size_t max_item_size;
};

static void *thread_func(void *const ctx)
{
  static const size_t batch_requests_count = 10000;

  struct thread_task *const task = ctx;

  for (;;) {
    size_t requests_count;

    /*
     * Grab and process requests in batches.
     */
    m_lock_lock(&task->lock);
    if (task->requests_count > batch_requests_count) {
      requests_count = batch_requests_count;
    }
    else {
      requests_count = task->requests_count;
    }
    task->requests_count -= requests_count;
    m_lock_unlock(&task->lock);

    if (requests_count == 0) {
      break;
    }

    simple_add(task->cache, requests_count, task->items_count,
        task->max_item_size);

    simple_get(task->cache, requests_count, task->get_items_count,
        task->max_item_size);
  }

  return NULL;
}

static void measure_multithreaded_ops(struct ybc *const cache,
    const size_t threads_count, const size_t requests_count,
    const size_t items_count, const size_t hot_items_count,
    const size_t max_item_size)
{
  double start_time, end_time;
  double qps;

  m_open(cache, items_count, hot_items_count, max_item_size);

  struct thread threads[threads_count];

  struct thread_task task = {
      .cache = cache,
      .requests_count = requests_count,
      .items_count = items_count,
      .get_items_count = hot_items_count ? hot_items_count : items_count,
      .max_item_size = max_item_size,
  };

  m_lock_init(&task.lock);

  start_time = m_get_current_time();
  for (size_t i = 0; i < threads_count; ++i) {
    start_thread(&threads[i], thread_func, &task);
  }

  for (size_t i = 0; i < threads_count; ++i) {
    join_thread(&threads[i]);
  }
  end_time = m_get_current_time();

  m_lock_destroy(&task.lock);

  qps = 2 * requests_count / (end_time - start_time);
  printf("multithreaded_ops(threads_count=%zu, requests_count=%zu, "
      "items_count=%zu, hot_items_count=%zu, max_item_size=%zu): %.2f qps\n",
      threads_count, requests_count, items_count, hot_items_count,
      max_item_size, qps);

  ybc_close(cache);
}

int main(void)
{
  char cache_buf[ybc_get_size()];
  struct ybc *const cache = (struct ybc *)cache_buf;

  const size_t requests_count = 4 * 1000 * 1000;
  const size_t items_count = 200 * 1000;

  for (size_t max_item_size = 8; max_item_size <= 4096; max_item_size *= 2) {

    measure_simple_ops(cache, requests_count, items_count, 0, max_item_size);

    for (size_t hot_items_count = 1000; hot_items_count <= items_count;
        hot_items_count *= 10) {
      measure_simple_ops(cache, requests_count, items_count, hot_items_count,
          max_item_size);
    }

    if (ybc_is_thread_safe()) {
      for (size_t threads_count = 1; threads_count <= 8; threads_count *= 2) {
        measure_multithreaded_ops(cache, threads_count, requests_count,
            items_count, 10 * 1000, max_item_size);
      }
    }
  }

  printf("All performance tests done\n");
  return 0;
}
