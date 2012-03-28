#include "../ybc.h"

/* Since tests rely on assert(), NDEBUG must be undefined. */
#undef NDEBUG

#include <assert.h>
#include <stdio.h>   /* printf, fopen, fclose, fwrite */
#include <stdlib.h>  /* malloc, free */
#include <string.h>  /* memcmp, memcpy, memset */

#ifdef YBC_HAVE_NANOSLEEP

#include <time.h>  /* nanosleep */

#define M_ERROR(error_message)  assert(0 && (error_message))

static void m_sleep(const uint64_t sleep_time)
{
  struct timespec req = {
      .tv_sec = sleep_time / 1000,
      .tv_nsec = (sleep_time % 1000) * 1000 * 1000,
  };

  const int rv = nanosleep(&req, NULL);
  assert(rv != -1);
}

#else  /* !YBC_HAVE_NANOSLEEP */
#error "Unsupported sleep implementation"
#endif

static void test_anonymous_cache_create(struct ybc *const cache)
{
  /* Non-forced open must fail. */
  if (ybc_open(cache, NULL, 0)) {
    M_ERROR("anonymous cache shouldn't be opened without force");
  }

  /* Forced open must succeed. */
  if (!ybc_open(cache, NULL, 1)) {
    M_ERROR("cannot open anonymous cache with force");
  }
  ybc_close(cache);
}

static void m_open_anonymous(struct ybc *const cache)
{
  if (!ybc_open(cache, NULL, 1)) {
    M_ERROR("cannot open anonymous cache");
  }
}

static void test_persistent_cache_create(struct ybc *const cache)
{
  char config_buf[ybc_config_get_size()];
  struct ybc_config *const config = (struct ybc_config *)config_buf;

  ybc_config_init(config);

  ybc_config_set_index_file(config, "./tmp_cache.index");
  ybc_config_set_data_file(config, "./tmp_cache.data");
  ybc_config_set_max_items_count(config, 1000);
  ybc_config_set_data_file_size(config, 1024 * 1024);

  /* Non-forced open must fail. */
  if (ybc_open(cache, config, 0)) {
    M_ERROR("non-existing persistent cache shouldn't be opened "
        "without force");
  }

  /* Forced open must succeed. */
  if (!ybc_open(cache, config, 1)) {
    M_ERROR("cannot create persistent cache");
  }
  ybc_close(cache);

  /* Non-forced open must succeed now. */
  if (!ybc_open(cache, config, 0)) {
    M_ERROR("cannot open existing persistent cache");
  }
  ybc_close(cache);

  /* Remove files associated with the cache. */
  ybc_remove(config);

  /* Non-forced open must fail again. */
  if (ybc_open(cache, config, 0)) {
    M_ERROR("non-existing persistent cache shouldn't be opened "
        "without force");
  }

  ybc_config_destroy(config);
}

static void expect_value(struct ybc_item *const item,
    const struct ybc_value *const expected_value)
{
  struct ybc_value actual_value;

  ybc_item_get_value(item, &actual_value);

  assert(actual_value.size == expected_value->size);
  assert(
      memcmp(actual_value.ptr, expected_value->ptr, actual_value.size) == 0);
  assert(actual_value.ttl <= expected_value->ttl);
}

static void expect_item_miss(struct ybc *const cache,
    const struct ybc_key *const key)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;

  if (ybc_item_acquire(cache, item, key)) {
    M_ERROR("unexpected item found");
  }
}

static void expect_item_hit(struct ybc *const cache,
    const struct ybc_key *const key,
    const struct ybc_value *const expected_value)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;

  if (!ybc_item_acquire(cache, item, key)) {
    M_ERROR("cannot find expected item");
  }
  expect_value(item, expected_value);
  ybc_item_release(item);
}

static void expect_item_add(struct ybc *const cache,
    const struct ybc_key *const key, const struct ybc_value *const value)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;

  if (!ybc_item_add(cache, item, key, value)) {
    M_ERROR("error when adding item");
  }
  expect_value(item, value);
  ybc_item_release(item);
  expect_item_hit(cache, key, value);
}

static void expect_item_remove(struct ybc *const cache,
    struct ybc_key *const key)
{
  ybc_item_remove(cache, key);
  expect_item_miss(cache, key);
}

static void expect_item_miss_de(struct ybc *const cache,
    const struct ybc_key *const key, const size_t grace_ttl)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;

  if (ybc_item_acquire_de(cache, item, key, grace_ttl)) {
    M_ERROR("unexpected item found");
  }
}

static void expect_item_hit_de(struct ybc *const cache,
    const struct ybc_key *const key,
    const struct ybc_value *const expected_value, const size_t grace_ttl)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;

  if (!ybc_item_acquire_de(cache, item, key, grace_ttl)) {
    M_ERROR("cannot find expected item");
  }
  expect_value(item, expected_value);
  ybc_item_release(item);
}

static void test_add_txn_rollback(struct ybc *const cache,
    struct ybc_add_txn *const txn, const struct ybc_key *const key,
    const size_t value_size)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;

  if (!ybc_add_txn_begin(cache, txn, item, key, value_size)) {
    M_ERROR("error when starting add transaction");
  }
  ybc_item_release(item);

  expect_item_miss(cache, key);
}

static void test_add_txn_commit(struct ybc *const cache,
    struct ybc_add_txn *const txn, const struct ybc_key *const key,
    const struct ybc_value *const value)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;

  if (!ybc_add_txn_begin(cache, txn, item, key, value->size)) {
    M_ERROR("error when starting add transaction");
  }

  void *const value_ptr = ybc_add_txn_get_value_ptr(txn);
  assert(value_ptr != NULL);
  memcpy(value_ptr, value->ptr, value->size);

  ybc_add_txn_commit(txn, value->ttl);

  expect_value(item, value);
  ybc_item_release(item);

  expect_item_hit(cache, key, value);
}

static void test_add_txn_failure(struct ybc *const cache,
    struct ybc_add_txn *const txn, const struct ybc_key *const key,
    const size_t value_size)
{
  char item_buf[ybc_item_get_size()];
  struct ybc_item *const item = (struct ybc_item *)item_buf;

  if (ybc_add_txn_begin(cache, txn, item, key, value_size)) {
    M_ERROR("unexpected transaction success");
  }
}

static void test_add_txn_ops(struct ybc *const cache)
{
  m_open_anonymous(cache);

  char add_txn_buf[ybc_add_txn_get_size()];
  struct ybc_add_txn *const txn = (struct ybc_add_txn *)add_txn_buf;

  struct ybc_key key = {
      .ptr = "abc",
      .size = 3,
  };

  struct ybc_value value = {
      .ptr = "qwerty",
      .size = 6,
      .ttl = 1000 * 1000,
  };

  test_add_txn_rollback(cache, txn, &key, value.size);

  test_add_txn_commit(cache, txn, &key, &value);

  /* Test zero-length key. */
  key.size = 0;
  test_add_txn_commit(cache, txn, &key, &value);

  /* Test zero-length value. */
  value.size = 0;
  test_add_txn_commit(cache, txn, &key, &value);

  /* Test too large key. */
  value.size = 6;
  key.size = SIZE_MAX;
  test_add_txn_failure(cache, txn, &key, value.size);

  /* Test too large value. */
  key.size = 3;
  test_add_txn_failure(cache, txn, &key, SIZE_MAX);
  test_add_txn_failure(cache, txn, &key, SIZE_MAX / 2);

  ybc_close(cache);
}

static void test_item_ops(struct ybc *const cache,
    const size_t iterations_count)
{
  m_open_anonymous(cache);

  struct ybc_key key;
  struct ybc_value value;

  value.ttl = 1000 * 1000;

  for (size_t i = 0; i < iterations_count; ++i) {
    key.ptr = &i;
    key.size = sizeof(i);

    expect_item_miss(cache, &key);
  }

  for (size_t i = 0; i < iterations_count; ++i) {
    key.ptr = &i;
    key.size = sizeof(i);
    value.ptr = &i;
    value.size = sizeof(i);

    expect_item_add(cache, &key, &value);
  }

  for (size_t i = 0; i < iterations_count; ++i) {
    key.ptr = &i;
    key.size = sizeof(i);

    expect_item_remove(cache, &key);
  }

  for (size_t i = 0; i < iterations_count; ++i) {
    key.ptr = &i;
    key.size = sizeof(i);

    expect_item_miss(cache, &key);
  }

  ybc_close(cache);
}

static void test_expiration(struct ybc *const cache)
{
  m_open_anonymous(cache);

  const struct ybc_key key = {
      .ptr = "aaa",
      .size = 3,
  };
  const struct ybc_value value = {
      .ptr = "1234",
      .size = 4,
      .ttl = 200,
  };
  expect_item_add(cache, &key, &value);

  m_sleep(300);

  /* The item should expire now. */
  expect_item_miss(cache, &key);

  ybc_close(cache);
}

static void test_dogpile_effect_ops(struct ybc *const cache)
{
  m_open_anonymous(cache);

  struct ybc_key key = {
      .ptr = "foo",
      .size = 3,
  };
  const struct ybc_value value = {
      .ptr = "bar",
      .size = 3,
      .ttl = 1000 * 1000,
  };

  /*
   * De-aware method should return an empty item on the first try
   * for non-existing item. The second try for the same non-existing item
   * will result in waiting for up to grace ttl period of time.
   */
  expect_item_miss_de(cache, &key, 200);

  /* Will wait for 200 milliseconds. */
  expect_item_miss_de(cache, &key, 10 * 1000);

  key.ptr = "bar";
  expect_item_add(cache, &key, &value);

  /*
   * If grace ttl is smaller than item's ttl, then the item should be returned.
   */
  expect_item_hit_de(cache, &key, &value, value.ttl / 100);

  /*
   * If grace ttl is larger than item's ttl, then an empty item
   * should be returned on the first try and the item itself should be returned
   * on subsequent tries irregardless of grace ttl value.
   */
  expect_item_miss_de(cache, &key, value.ttl * 100);
  expect_item_hit_de(cache, &key, &value, value.ttl * 100);
  expect_item_hit_de(cache, &key, &value, value.ttl / 100);

  ybc_close(cache);
}

static void test_cluster_ops(const size_t cluster_size,
    const size_t iterations_count)
{
  char configs_buf[ybc_config_get_size() * cluster_size];
  struct ybc_config *const configs = (struct ybc_config *)configs_buf;

  for (size_t i = 0; i < cluster_size; ++i) {
    ybc_config_init(YBC_CONFIG_GET(configs_buf, i));
  }

  char cluster_buf[ybc_cluster_get_size(cluster_size)];
  struct ybc_cluster *const cluster = (struct ybc_cluster *)cluster_buf;

  /* Unfored open must fail. */
  if (ybc_cluster_open(cluster, configs, cluster_size, 0)) {
    M_ERROR("cache cluster shouldn't be opened without force");
  }

  /* Forced open must succeed. */
  if (!ybc_cluster_open(cluster, configs, cluster_size, 1)) {
    M_ERROR("failed opening cache cluster");
  }

  /* Configs are no longer needed, so they can be destroyed. */
  for (size_t i = 0; i < cluster_size; ++i) {
    ybc_config_destroy(YBC_CONFIG_GET(configs_buf, i));
  }

  struct ybc_key key;
  struct ybc_value value;

  value.ttl = 1000 * 1000;

  for (size_t i = 0; i < iterations_count; ++i) {
    key.ptr = &i;
    key.size = sizeof(i);
    value.ptr = &i;
    value.size = sizeof(i);

    struct ybc *const cache = ybc_cluster_get_cache(cluster, &key);
    expect_item_add(cache, &key, &value);
  }

  ybc_cluster_close(cluster);
}

static void test_interleaved_adds(struct ybc *const cache)
{
  m_open_anonymous(cache);

  char add_txn1_buf[ybc_add_txn_get_size()];
  char add_txn2_buf[ybc_add_txn_get_size()];
  struct ybc_add_txn *const txn1 = (struct ybc_add_txn *)add_txn1_buf;
  struct ybc_add_txn *const txn2 = (struct ybc_add_txn *)add_txn2_buf;

  char item1_buf[ybc_item_get_size()];
  char item2_buf[ybc_item_get_size()];
  struct ybc_item *const item1 = (struct ybc_item *)item1_buf;
  struct ybc_item *const item2 = (struct ybc_item *)item2_buf;

  const struct ybc_key key1 = {
      .ptr = "foo",
      .size = 3,
  };
  const struct ybc_key key2 = {
      .ptr = "barz",
      .size = 4,
  };

  const struct ybc_value value1 = {
      .ptr = "123456",
      .size = 6,
      .ttl = 1000 * 1000,
  };
  const struct ybc_value value2 = {
      .ptr = "qwert",
      .size = 4,
      .ttl = 1000 * 1000,
  };

  if (!ybc_add_txn_begin(cache, txn1, item1, &key1, value1.size)) {
    M_ERROR("Cannot start the first add transaction");
  }

  if (!ybc_add_txn_begin(cache, txn2, item2, &key2, value2.size)) {
    M_ERROR("Cannot start the second add transaction");
  }

  memcpy(ybc_add_txn_get_value_ptr(txn1), value1.ptr, value1.size);
  memcpy(ybc_add_txn_get_value_ptr(txn2), value2.ptr, value2.size);

  expect_item_miss(cache, &key1);
  expect_item_miss(cache, &key2);

  ybc_add_txn_commit(txn1, value1.ttl);
  ybc_add_txn_commit(txn2, value2.ttl);

  expect_value(item1, &value1);
  expect_value(item2, &value2);

  ybc_item_release(item1);
  ybc_item_release(item2);

  expect_item_hit(cache, &key1, &value1);
  expect_item_hit(cache, &key2, &value2);

  ybc_close(cache);
}

static void test_instant_clear(struct ybc *const cache)
{
  char config_buf[ybc_config_get_size()];
  struct ybc_config *const config = (struct ybc_config *)config_buf;

  ybc_config_init(config);

  ybc_config_set_max_items_count(config, 1000);
  ybc_config_set_data_file_size(config, 128 * 1024);

  if (!ybc_open(cache, config, 1)) {
    M_ERROR("cannot create anonymous cache");
  }

  ybc_config_destroy(config);

  struct ybc_key key;
  struct ybc_value value;

  /* Add a lot of items to the cache */
  value.ttl = 1000 * 1000;
  for (size_t i = 0; i < 1000; ++i) {
    key.ptr = &i;
    key.size = sizeof(i);
    value.ptr = &i;
    value.size = sizeof(i);
    expect_item_add(cache, &key, &value);
  }

  ybc_clear(cache);

  /* Test that the cache doesn't contain any items after the clearance. */
  for (size_t i = 0; i < 1000; ++i) {
    key.ptr = &i;
    key.size = sizeof(i);
    expect_item_miss(cache, &key);
  }

  ybc_close(cache);
}

static void test_persistent_survival(struct ybc *const cache)
{
  char config_buf[ybc_config_get_size()];
  struct ybc_config *const config = (struct ybc_config *)config_buf;

  ybc_config_init(config);

  ybc_config_set_index_file(config, "./tmp_cache.index");
  ybc_config_set_data_file(config, "./tmp_cache.data");
  ybc_config_set_max_items_count(config, 10);
  ybc_config_set_data_file_size(config, 1024);

  if (!ybc_open(cache, config, 1)) {
    M_ERROR("cannot create persistent cache");
  }

  const struct ybc_key key = {
      .ptr = "foobar",
      .size = 6.
  };
  const struct ybc_value value = {
      .ptr = "qwert",
      .size = 5,
      .ttl = 1000 * 1000,
  };
  expect_item_add(cache, &key, &value);

  ybc_close(cache);

  /* Re-open the same cache and make sure the item exists there. */

  if (!ybc_open(cache, config, 0)) {
    M_ERROR("cannot open persistent cache");
  }

  expect_item_hit(cache, &key, &value);

  ybc_close(cache);

  ybc_remove(config);

  ybc_config_destroy(config);
}

static void test_broken_index_handling(struct ybc *const cache)
{
  char config_buf[ybc_config_get_size()];
  struct ybc_config *const config = (struct ybc_config *)config_buf;

  ybc_config_init(config);

  ybc_config_set_index_file(config, "./tmp_cache.index");
  ybc_config_set_data_file(config, "./tmp_cache.data");
  ybc_config_set_max_items_count(config, 1000);
  ybc_config_set_data_file_size(config, 64 * 1024);

  /* Create index and data files. */
  if (!ybc_open(cache, config, 1)) {
    M_ERROR("cannot create persistent cache");
  }

  ybc_close(cache);

  /* Corrupt index file. */
  FILE *const fp = fopen("./tmp_cache.index", "r+");
  for (size_t i = 0; i < 100; ++i) {
    if (fwrite(&i, sizeof(i), 1, fp) != 1) {
      M_ERROR("cannot write data");
    }
  }
  fclose(fp);

  /* Try reading index file. It must be "empty". */
  if (!ybc_open(cache, config, 0)) {
    M_ERROR("cannot open persistent cache");
  }

  struct ybc_key key;
  for (size_t i = 0; i < 100; ++i) {
    key.ptr = &i;
    key.size = sizeof(i);
    expect_item_miss(cache, &key);
  }

  ybc_close(cache);

  /* Remove index and data files. */
  ybc_remove(config);

  ybc_config_destroy(config);
}

void test_large_cache(struct ybc *const cache)
{
  char config_buf[ybc_config_get_size()];
  struct ybc_config *const config = (struct ybc_config *)config_buf;

  ybc_config_init(config);

  ybc_config_set_max_items_count(config, 10 * 1000);
  ybc_config_set_data_file_size(config, 32 * 1024 * 1024);

  if (!ybc_open(cache, config, 1)) {
    M_ERROR("cannot create anonymous cache");
  }

  ybc_config_destroy(config);

  const size_t value_buf_size = 13 * 3457;

  char *const value_buf = malloc(value_buf_size);
  memset(value_buf, 'q', value_buf_size);

  struct ybc_key key;
  const struct ybc_value value = {
    .ptr = value_buf,
    .size = value_buf_size,
    .ttl = 1000 * 1000,
  };

  /* Test handling of cache data size wrapping. */
  for (size_t i = 0; i < 10 * 1000; ++i) {
    key.ptr = &i;
    key.size = sizeof(i);
    expect_item_add(cache, &key, &value);
  }

  free(value_buf);

  ybc_close(cache);
}

static void test_small_sync_interval(struct ybc *const cache)
{
  char config_buf[ybc_config_get_size()];
  struct ybc_config *const config = (struct ybc_config *)config_buf;

  ybc_config_init(config);

  ybc_config_set_max_items_count(config, 100);
  ybc_config_set_data_file_size(config, 4000);
  ybc_config_set_sync_interval(config, 100);

  if (!ybc_open(cache, config, 1)) {
    M_ERROR("cannot create anonymous cache");
  }

  ybc_config_destroy(config);

  struct ybc_key key;
  const struct ybc_value value = {
      .ptr = "1234567890a",
      .size = 11,
      .ttl = 1000 * 1000,
  };

  for (size_t i = 0; i < 10; ++i) {
    for (size_t j = 0; j < 100; ++j) {
      key.ptr = &j;
      key.size = sizeof(j);
      expect_item_add(cache, &key, &value);
    }
    m_sleep(31);
  }

  ybc_close(cache);
}

int main(void)
{
  char cache_buf[ybc_get_size()];
  struct ybc *const cache = (struct ybc *)cache_buf;

  test_anonymous_cache_create(cache);
  test_persistent_cache_create(cache);

  test_add_txn_ops(cache);
  test_item_ops(cache, 1000);
  test_expiration(cache);
  test_dogpile_effect_ops(cache);
  test_cluster_ops(5, 1000);

  test_interleaved_adds(cache);
  test_instant_clear(cache);
  test_persistent_survival(cache);
  test_broken_index_handling(cache);
  test_large_cache(cache);
  test_small_sync_interval(cache);

  printf("All functional tests done\n");
  return 0;
}
