/*
 * Performance tests.
 */

#include "../platform.h"
#include "../ybc.h"

#include <assert.h>
#include <stdint.h>  /* uint*_t */
#include <stdio.h>   /* printf */
#include <stdlib.h>  /* exit, free */
#include <string.h>  /* memset, memcmp */


#define M_ERROR(error_message)  do { \
  fprintf(stderr, "%s\n", error_message); \
  exit(EXIT_FAILURE); \
} while (0)

struct m_rand_state
{
  uint64_t s;
};

static void m_rand_init(struct m_rand_state *const rs)
{
  rs->s = p_get_current_time();
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
      fprintf(stderr, "Expected %d char, but obtained %d at position %zu. object_size=%zu\n",
          (int)c, (int)ss[i], i, n);
      return 0;
    }
  }
  return 1;
}

static void simple_set(struct ybc *const cache, const size_t requests_count,
    const size_t items_count, const size_t max_item_size)
{
  struct m_rand_state rand_state;
  uint64_t tmp;

  char *const buf = p_malloc(max_item_size);

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

    if (!ybc_item_set(cache, &key, &value)) {
      M_ERROR("Cannot store item in the cache");
    }
  }

  p_free(buf);
}

static void simple_get_miss(struct ybc *const cache,
    const size_t requests_count, const size_t items_count)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;
  struct m_rand_state rand_state;
  uint64_t tmp;

  const struct ybc_key key = {
      .ptr = &tmp,
      .size = sizeof(tmp),
  };

  m_rand_init(&rand_state);

  for (size_t i = 0; i < requests_count; ++i) {
    tmp = m_rand_next(&rand_state) % items_count;

    if (ybc_item_get(cache, item, &key)) {
      M_ERROR("Unexpected item found");
    }
  }
}

static void simple_get_hit(struct ybc *const cache,
    const size_t requests_count, const size_t items_count,
    const size_t max_item_size)
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

    if (ybc_item_get(cache, item, &key)) {
      /* Emulate access to the item */
      ybc_item_get_value(item, &value);
      if (value.size > max_item_size) {
        M_ERROR("Unexpected value size");
      }
      if (!m_memset_check(value.ptr, (char)value.size, value.size)) {
        fprintf(stderr, "i=%zu, requests_count=%zu, value.size=%zu\n", i, requests_count, value.size);
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

  printf("simple_ops(requests=%zu, items=%zu, "
      "hot_items=%zu, max_item_size=%zu)\n",
      requests_count, items_count, hot_items_count, max_item_size);

  start_time = p_get_current_time();
  simple_get_miss(cache, requests_count, items_count);
  end_time = p_get_current_time();
  qps = requests_count / (end_time - start_time) * 1000;
  printf("  get_miss: %.02f qps\n", qps);

  start_time = p_get_current_time();
  simple_set(cache, requests_count, items_count, max_item_size);
  end_time = p_get_current_time();
  qps = requests_count / (end_time - start_time) * 1000;
  printf("  set     : %.02f qps\n", qps);

  const size_t get_items_count = hot_items_count ? hot_items_count :
      items_count;
  start_time = p_get_current_time();
  simple_get_hit(cache, requests_count, get_items_count, max_item_size);
  end_time = p_get_current_time();
  qps = requests_count / (end_time - start_time) * 1000;
  printf("  get_hit : %.02f qps\n", qps);

  ybc_close(cache);
}

struct thread_task
{
  struct p_lock lock;
  struct ybc *cache;
  size_t requests_count;
  size_t items_count;
  size_t get_items_count;
  size_t max_item_size;
};

static size_t get_batch_requests_count(struct thread_task *const task)
{
    static const size_t batch_requests_count = 10000;
    size_t requests_count = batch_requests_count;

    p_lock_lock(&task->lock);
    if (task->requests_count < batch_requests_count) {
      requests_count = task->requests_count;
    }
    task->requests_count -= requests_count;
    p_lock_unlock(&task->lock);

    return requests_count;
}

static void thread_func_set(void *const ctx)
{
  struct thread_task *const task = ctx;
  for (;;) {
    const size_t requests_count = get_batch_requests_count(task);
    if (requests_count == 0) {
      break;
    }
    simple_set(task->cache, requests_count, task->items_count,
        task->max_item_size);
  }
}

static void thread_func_get_miss(void *const ctx)
{
  struct thread_task *const task = ctx;
  for (;;) {
    const size_t requests_count = get_batch_requests_count(task);
    if (requests_count == 0) {
      break;
    }
    simple_get_miss(task->cache, requests_count, task->get_items_count);
  }
}

static void thread_func_get_hit(void *const ctx)
{
  struct thread_task *const task = ctx;
  for (;;) {
    const size_t requests_count = get_batch_requests_count(task);
    if (requests_count == 0) {
      break;
    }
    simple_get_hit(task->cache, requests_count, task->get_items_count,
        task->max_item_size);
  }
}

static void thread_func_set_get(void *const ctx)
{
  struct thread_task *const task = ctx;
  for (;;) {
    const size_t requests_count = get_batch_requests_count(task);
    if (requests_count == 0) {
      break;
    }
    const size_t set_requests_count = (size_t)(requests_count * 0.1);
    const size_t get_requests_count = requests_count - set_requests_count;

    simple_set(task->cache, set_requests_count, task->items_count,
        task->max_item_size);
    simple_get_hit(task->cache, get_requests_count, task->get_items_count,
        task->max_item_size);
  }
}

static double measure_qps(struct thread_task *const task,
    const p_thread_func thread_func, const size_t threads_count,
    const size_t requests_count)
{
  struct p_thread threads[threads_count];
  task->requests_count = requests_count;

  double start_time = p_get_current_time();
  for (size_t i = 0; i < threads_count; ++i) {
    p_thread_init_and_start(&threads[i], thread_func, task);
  }

  for (size_t i = 0; i < threads_count; ++i) {
    p_thread_join_and_destroy(&threads[i]);
  }
  double end_time = p_get_current_time();

  return requests_count / (end_time - start_time) * 1000;
}

static void measure_multithreaded_ops(struct ybc *const cache,
    const size_t threads_count, const size_t requests_count,
    const size_t items_count, const size_t hot_items_count,
    const size_t max_item_size)
{
  double qps;

  m_open(cache, items_count, hot_items_count, max_item_size);

  struct thread_task task = {
      .cache = cache,
      .items_count = items_count,
      .get_items_count = hot_items_count ? hot_items_count : items_count,
      .max_item_size = max_item_size,
  };

  p_lock_init(&task.lock);

  printf("multithreaded_ops(requests=%zu, items=%zu, hot_items=%zu, "
      "max_item_size=%zu, threads=%zu)\n",
      requests_count, items_count, hot_items_count, max_item_size,
      threads_count);

  qps = measure_qps(&task, thread_func_get_miss, threads_count, requests_count);
  printf("  get_miss: %.2f qps\n", qps);

  qps = measure_qps(&task, thread_func_set, threads_count, requests_count);
  printf("  set     : %.2f qps\n", qps);

  qps = measure_qps(&task, thread_func_get_hit, threads_count, requests_count);
  printf("  get_hit : %.2f qps\n", qps);

  qps = measure_qps(&task, thread_func_set_get, threads_count, requests_count);
  printf("  get_set : %.2f qps\n", qps);

  p_lock_destroy(&task.lock);

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

    for (size_t threads_count = 1; threads_count <= 16; threads_count *= 2) {
      measure_multithreaded_ops(cache, threads_count, requests_count,
          items_count, 10 * 1000, max_item_size);
    }
  }

  printf("All performance tests done\n");
  return 0;
}
