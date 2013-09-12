#include "ybc.h"

static int go_get_item_and_value(struct ybc *const cache,
    struct ybc_item *const item, struct ybc_value *const value,
    const struct ybc_key *const key)
{
  const int rv = ybc_item_get(cache, item, key);
  if (rv == 0) {
    return 0;
  }
  ybc_item_get_value(item, value);
  return rv;
}

static enum ybc_de_status go_get_item_and_value_de_async(
    struct ybc *const cache,
    struct ybc_item *const item, struct ybc_value *const value,
    const struct ybc_key *const key, const uint64_t grace_ttl)
{
  const enum ybc_de_status rv = ybc_item_get_de_async(cache, item, key,
      grace_ttl);
  if (rv != YBC_DE_SUCCESS) {
    return rv;
  }
  ybc_item_get_value(item, value);
  return rv;
}

static int go_set_item_and_value(struct ybc *const cache,
    struct ybc_item *const item, const struct ybc_key *const key,
    struct ybc_value *const value)
{
  const int rv = ybc_item_set_item(cache, item, key, value);
  if (rv == 0) {
    return 0;
  }
  ybc_item_get_value(item, value);
  return rv;
}

static void go_commit_item_and_value(struct ybc_set_txn *const txn,
    struct ybc_item *const item, struct ybc_value *const value)
{
  ybc_set_txn_commit_item(txn, item);
  ybc_item_get_value(item, value);
}

