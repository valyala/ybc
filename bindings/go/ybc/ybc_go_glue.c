#include "ybc.h"

static int go_item_set(struct ybc *cache,
    const void *const key_ptr, const size_t key_size,
    void *const value_ptr, const size_t value_size, const uint64_t value_ttl)
{
  const struct ybc_key key = {
    .ptr = key_ptr,
    .size = key_size,
  };
  const struct ybc_value value = {
    .ptr = value_ptr,
    .size = value_size,
    .ttl = value_ttl,
  };

  return ybc_item_set(cache, &key, &value);
}

static int go_item_remove(struct ybc *cache,
    const void *const key_ptr, const size_t key_size)
{
  const struct ybc_key key = {
    .ptr = key_ptr,
    .size = key_size,
  };

  return ybc_item_remove(cache, &key);
}

static int go_simple_set(struct ybc *cache,
    const void *const key_ptr, const size_t key_size,
    void *const value_ptr, const size_t value_size, const uint64_t value_ttl)
{
  const struct ybc_key key = {
    .ptr = key_ptr,
    .size = key_size,
  };
  const struct ybc_value value = {
    .ptr = value_ptr,
    .size = value_size,
    .ttl = value_ttl,
  };

  return ybc_simple_set(cache, &key, &value);
}

struct go_ret_size {
  size_t size;
  int result;
};

static struct go_ret_size go_simple_get(struct ybc *const cache,
    const void *const key_ptr, const size_t key_size,
    void *const value_ptr, const size_t value_size)
{
  struct go_ret_size rv;
  const struct ybc_key key = {
    .ptr = key_ptr,
    .size = key_size,
  };
  struct ybc_value value = {
    .ptr = value_ptr,
    .size = value_size,
  };

  rv.result = ybc_simple_get(cache, &key, &value);
  rv.size = value.size;

  return rv;
}

static int go_set_txn_begin(struct ybc *const cache,
    struct ybc_set_txn *const txn,
    const void *const key_ptr, const size_t key_size,
    const size_t value_size, const uint64_t ttl)
{
  const struct ybc_key key = {
    .ptr = key_ptr,
    .size = key_size,
  };

  return ybc_set_txn_begin(cache, txn, &key, value_size, ttl);
}

struct go_ret_value {
  struct ybc_value value;
  int result;
};

static struct go_ret_value go_get_item_and_value(struct ybc *const cache,
    struct ybc_item *const item,
    const void *const key_ptr, const size_t key_size)
{
  struct go_ret_value rv;
  const struct ybc_key key = {
    .ptr = key_ptr,
    .size = key_size,
  };

  rv.result = ybc_item_get(cache, item, &key);
  if (rv.result != 0) {
    ybc_item_get_value(item, &rv.value);
  }

  return rv;
}

struct go_ret_de_value {
  struct ybc_value value;
  enum ybc_de_status status;
};

static struct go_ret_de_value go_get_item_and_value_de_async(
    struct ybc *const cache, struct ybc_item *const item,
    const void *const key_ptr, const size_t key_size,
    const uint64_t grace_ttl)
{
  struct go_ret_de_value rv;
  const struct ybc_key key = {
    .ptr = key_ptr,
    .size = key_size,
  };

  rv.status = ybc_item_get_de_async(cache, item, &key, grace_ttl);
  if (rv.status == YBC_DE_SUCCESS) {
    ybc_item_get_value(item, &rv.value);
  }

  return rv;
}

static struct go_ret_value go_set_item_and_value(struct ybc *const cache,
    struct ybc_item *const item,
    const void *const key_ptr, const size_t key_size,
    void *const value_ptr, const size_t value_size, const uint64_t value_ttl)
{
  struct go_ret_value rv;
  const struct ybc_key key = {
    .ptr = key_ptr,
    .size = key_size,
  };
  const struct ybc_value value = {
    .ptr = value_ptr,
    .size = value_size,
    .ttl = value_ttl,
  };

  rv.result = ybc_item_set_item(cache, item, &key, &value);
  if (rv.result != 0) {
    ybc_item_get_value(item, &rv.value);
  }

  return rv;
}

static struct ybc_value go_commit_item_and_value(struct ybc_set_txn *const txn,
    struct ybc_item *const item)
{
  ybc_set_txn_commit_item(txn, item);

  struct ybc_value value;
  ybc_item_get_value(item, &value);
  return value;
}
