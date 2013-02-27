#ifndef YBC_H_INCLUDED
#define YBC_H_INCLUDED

#include <stddef.h>  /* for size_t */
#include <stdint.h>  /* for uint*_t */


#ifdef __cplusplus
extern "C" {
#endif

#if defined(YBC_BUILD_LIBRARY)
#define YBC_API  __attribute__((externally_visible))
#else
#define YBC_API  /* No special handling for static linkage. */
#endif

/*
 * Maximum ttl.
 *
 * Use this ttl when adding an item without expiration time. Such item will live
 * as long as possible. But this doesn't mean the item will stay in the cache
 * forever. It may be removed from the cache at any time, but usually it lives
 * longer than items with smaller ttls.
 */
#define YBC_MAX_TTL (~(uint64_t)0)

/*******************************************************************************
 * Config API.
 *
 * Config object allows overriding default settings when opening a cache.
 *
 * Usage:
 *
 * char config_buf[ybc_config_get_size()];
 * struct ybc_config *const config = (struct ybc_config *)config_buf;
 * ...
 * ybc_config_init(config);
 * ybc_config_set_max_items_count(config, 1000 * 1000);
 * ybc_config_set_data_file_size(config, (size_t)8 * 1024 * 1024 * 1024);
 * ...
 * create_and_use_cache(config);
 * ...
 * ybc_config_destroy(config);
 ******************************************************************************/

/*
 * Cache configuration handler.
 */
struct ybc_config;

/*
 * Returns the size of ybc_config structure in bytes.
 *
 * The caller is responsible for allocating this amount of memory
 * for the structure before passing it into ybc_config_*() functions.
 */
YBC_API size_t ybc_config_get_size(void);

/*
 * Initializes the given config and sets all settings to default values.
 *
 * The initialized config must be destroyed by ybc_config_destroy().
 */
YBC_API void ybc_config_init(struct ybc_config *config);

/*
 * Destroys the given config, which has been initialized by ybc_config_init().
 */
YBC_API void ybc_config_destroy(struct ybc_config *config);

/*
 * Sets the maximum number of items in the cache.
 *
 * Index file size will be proportional to this number.
 */
YBC_API void ybc_config_set_max_items_count(struct ybc_config *config,
    size_t max_items_count);

/*
 * Sets data file size in bytes.
 *
 * Keys and blobs are stored in the data file, so the size must be large enough
 * for storing the maximum expected number of items in the cache.
 *
 * The size can be arbitrary large. The only limit on the size is filesystem's
 * free space.
 */
YBC_API void ybc_config_set_data_file_size(struct ybc_config *config,
    size_t size);

/*
 * Sets path to the index file for the given config.
 *
 * The file contains index for fast lookup of item's value in data file
 * by item's key.
 *
 * If index file is not set or set to NULL, then it is automatically created
 * when the cache is opened and automatically deleted when the cache is closed.
 * This effectively disables cache persistence - i.e. the cache
 * will be flushed on the next opening even if data file is non-NULL.
 *
 * It is safe modifying the string pointed by filename after the call.
 *
 * Defragmented index file may lead to faster startup times, while the cache
 * is loading index data into memory.
 */
YBC_API void ybc_config_set_index_file(struct ybc_config *config,
    const char *filename);

/*
 * Sets path to the data file for the given config.
 *
 * Data file contains items' values.
 *
 * If data file is not set or set to NULL, then it is automatically created
 * when the cache is opened and automatically deleted when the cache is closed.
 * This effectively disables cache persistence - i.e. the cache
 * will be flushed on the next opening even if index file is non-NULL.
 *
 * It is safe modifying the string pointed by filename after the call.
 *
 * Data file has the following access pattern:
 * - almost sequential writes.
 * - random reads, which are grouped close to the last write position.
 *
 * Sequential writes eliminate disk seek latency, while random reads with good
 * locality of reference result in good OS file cache utilization.
 * Due to these properties data file can be placed on both HDDs and SSDs
 * in order to achieve high performance. SSD will be faster than HDD only
 * if frequently requested items don't fit OS file cache. Otherwise SSD
 * will be on par with HDD.
 *
 * Defragmented data file usually leads to higher cache performance, because
 * it minimizes random I/O.
 */
YBC_API void ybc_config_set_data_file(struct ybc_config *config,
    const char *filename);

/*
 * Sets the expected number of hot (frequently requested) items in the cache.
 *
 * If this number is much smaller than the actual number of hot items,
 * then the cache may experience some slowdown.
 *
 * If this number is much higher than the actual number of hot items,
 * then the cache may waste some memory.
 *
 * Setting this number to 0 disables hot items' cache.
 * This may be useful if the number of hot items is close to the maximum number
 * of items in the cache set via ybc_config_set_max_items_count().
 *
 * By default hot items' cache is enabled.
 *
 * Default value should work well for almost all cases, so tune this value only
 * if you know what you are doing.
 */
YBC_API void ybc_config_set_hot_items_count(struct ybc_config *config,
    size_t hot_items_count);

/*
 * Sets the expected size of hot (frequently requested) data in the cache.
 *
 * Hot data includes keys and values for hot items.
 *
 * If the expected size is smaller than the actual size of hot data,
 * then the cache may waste CPU time, memory bandwidth and I/O bandwidth
 * while trying to compact hot cache data.
 *
 * If the expected size is larger than the actual size of hot data,
 * then the cache may waste additional memory due to fragmented hot cache data.
 *
 * Setting this number to 0 disables hot data compaction.
 * This may be useful in the following cases:
 * - if only large blobs with more than 100Kb sizes are stored in the cache;
 * - if the size of hot data is close to the data file size
 *   set via ybc_config_set_data_file_size().
 *
 * By default hot data compaction is enabled.
 *
 * Default value should work well for almost all cases, so tune this value only
 * if you know what you are doing.
 */
YBC_API void ybc_config_set_hot_data_size(struct ybc_config *config,
    size_t hot_data_size);

/*
 * Sets the size of hashtable used for tracking pending items affected
 * by dogpile effect.
 *
 * The hashtable size should be close to the average number of distinct pending
 * items concurrently affected by dogpile effect (N_de).
 *
 * If N_de is much larger than the hashtable size, then ybc_item_get_de*() may
 * waste a lot of CPU time looking up pending items affected by dogpile effect.
 *
 * If N_de is smaller than the hashtable size, then the cache may waste
 * additional memory.
 *
 * Default value should work well for almost all cases, so tune this value only
 * if you know what you are doing.
 */
YBC_API void ybc_config_set_de_hashtable_size(struct ybc_config *config,
    size_t de_hashtable_size);

/*
 * Sets interval for cache data syncing in milliseconds.
 *
 * Cache items are periodically synced to data file with this interval.
 * Non-synced cache items may be lost after the program or system crash.
 *
 * While short sync interval reduces the number of lost items in the event
 * of program or system crash, it also increases the number of writes to data
 * file, which may slow down the program.
 *
 * Long sync interval minimizes the number of writes to data file at the cost
 * of potentially higher number of lost items in the event of program crash.
 *
 * Setting sync interval to 0 completely disables data syncing. Even if syncing
 * is disabled, the cache is persisted at ybc_close() call. The cache won't
 * persist only in the event of program crash before ybc_close() call.
 * By default syncing is enabled.
 *
 * Default value should work well for almost all cases, so tune this value only
 * if you know what you are doing.
 *
 * Periodic data syncing has also a nice side-effect - it minimizes the number
 * of dirty VM pages behind the cache using fast sequential write to backing
 * store. If periodic data syncing is disabled, then the Operating System should
 * decide when and in which order to sync those dirty pages to backing store.
 * This may be much slower if the OS syncs those dirty pages in random order.
 */
YBC_API void ybc_config_set_sync_interval(struct ybc_config *config,
    uint64_t sync_interval);

/*
 * Disables protection from items' overwrite corruption.
 *
 * By default the protection is enabled, so your data should always be
 * in a consistent state.
 *
 * If protection is disabled, then items' data may be corrupted by overwriting
 * old items' data by new items' data. This mode may be useful if you'll make
 * a copy of returned items' data into your memory and then verify this data
 * before passing it further. There are ybc_simple_set() and ybc_simple_get()
 * functions, which automatically perform items' data validation.
 *
 * This mode is safe if the folllowing conditions are met:
 *   * only read operations performed on the cache;
 *   * hot data compaction is disabled (see ybc_config_set_hot_data_size()
 *     for details).
 *
 * Read operations scale much better under this mode on multiple CPUs.
 */
YBC_API void ybc_config_disable_overwrite_protection(struct ybc_config *config);


/*******************************************************************************
 * Cache management API.
 *
 * Usage:
 *
 * char cache_buf[ybc_get_size()];
 * struct ybc *const cache = (struct ybc *)cache_buf;
 * ...
 * if (!ybc_open(cache, NULL, 1)) {
 *   log_error("error when opening cache");
 *   exit(EXIT_FAILURE);
 * }
 * ...
 * use_cache(cache);
 * ybc_close(cache);
 ******************************************************************************/

/*
 * Cache handler.
 */
struct ybc;

/*
 * Returns the size of ybc structure in bytes.
 *
 * The caller is responsible for allocating this amount of memory
 * for the structure before passing it into ybc_*() functions.
 */
YBC_API size_t ybc_get_size(void);

/*
 * Opens and, optionally, initializes the cache.
 *
 * The config must be initialized and adjusted with ybc_config_*() functions
 * before calling this function. The config may be destroyed after opening
 * the cache - there is no need in waiting until the cache will be closed.
 *
 * If config is NULL, then default configuration settings are used
 * and an anonymous cache is created. This cache is automatically destroyed
 * on closing.
 *
 * If force is set, the function can take non-trivial amount of time,
 * because it may by busy pre-allocating and initializing cache files.
 *
 * If force is set, then the function creates missing files and tries fixing
 * any errors found in these files.
 * Otherwise the function returns 0 if some files are missing or errors
 * are found in these files.
 *
 * Do not open the same cache files simultaneously from distinct threads
 * or processes!
 *
 * Returns non-zero value on success, 0 on error.
 */
YBC_API int ybc_open(struct ybc *cache, const struct ybc_config *config,
    int force);

/*
 * Closes the given cache handler.
 */
YBC_API void ybc_close(struct ybc *cache);

/*
 * Discards all the items in the cache.
 *
 * This function is very fast and its' speed doesn't depend on the number
 * and the size of items stored in the cache.
 *
 * Unlike ybc_remove(), this function doesn't remove files associated
 * with the cache.
 */
YBC_API void ybc_clear(struct ybc *cache);

/*
 * Removes files associated with the given cache.
 *
 * Do not call this function if the corresponding cache is open!
 *
 * Config must be non-NULL.
 */
YBC_API void ybc_remove(const struct ybc_config *config);


/*******************************************************************************
 * 'Add' transaction API.
 *
 * The API allows serializing item's value directly to cache storage instead
 * of serializing it to a temporary buffer before copying serialized contents
 * into the cache.
 *
 *
 * Usage:
 *
 * const struct ybc_key key = {
 *     .ptr = key_ptr,
 *     .size = key_size,
 * };
 * char set_txn_buf[ybc_set_txn_get_size()];
 * struct ybc_set_txn *const txn = (struct ybc_set_txn *)set_txn_buf;
 *
 * ...
 *
 * value_size = get_serialized_object_size(obj);
 * if (ybc_set_txn_begin(cache, txn, &key, value_size, ttl)) {
 *   struct ybc_set_txn_value txn_value;
 *   ybc_set_txn_get_value(txn, &txn_value);
 *   if (serialize_object(obj, txn_value.ptr)) {
 *     char item_buf[ybc_item_get_size()];
 *     struct ybc_item *const item = (struct ybc_item *)item_buf;
 *     struct ybc_value value;
 *
 *     ybc_set_txn_commit_item(txn, item);
 *
 *     ybc_item_get_value(item, &value);
 *
 *     use_value(&value);
 *
 *     ybc_item_release(item);
 *   }
 *   else {
 *     ybc_set_txn_rollback(txn);
 *   }
 * }
 *
 ******************************************************************************/

/*
 * Acquired item handler.
 */
struct ybc_item;

/*
 * 'Set' transaction handler.
 */
struct ybc_set_txn;

/*
 * Cache item's key.
 */
struct ybc_key
{
  /*
   * A pointer to key.
   */
  const void *ptr;

  /*
   * Key size in bytes.
   */
  size_t size;
};

/*
 * Value, which is returned by ybc_set_txn_get_value().
 */
struct ybc_set_txn_value
{
  /*
   * A pointer to the allocated region of memory.
   */
  void *ptr;

  /*
   * The size of allocated region of memory.
   */
  size_t size;
};

/*
 * Returns the size of ybc_set_txn structure in bytes.
 *
 * The caller is responsible for allocating this amount of memory
 * for the structure before passing it into ybc_set_txn_*() functions.
 *
 * Since the size of ybc_set_txn structure never exceeds a few hundred bytes,
 * it is usually safe allocating space for the structure on the stack.
 */
YBC_API size_t ybc_set_txn_get_size(void);

/*
 * Starts 'set' transaction for the given key and value of the given size.
 *
 * Allocates space in the cache for storing an item (key + value) and sets
 * the given ttl in milliseconds for the item. Set ttl to YBC_MAX_TTL
 * for items without expiration time.
 *
 * The caller is responsible for filling up value_size bytes returned
 * by ybc_set_txn_get_value() before commiting the transaction.
 *
 * The caller may freely modify key contents after the call to this function,
 * because the function makes an internal copy of the key.
 *
 * The transaction may be commited by calling ybc_set_txn_commit*()
 * or may be rolled back by calling ybc_set_txn_rollback().
 *
 * Returns non-zero on success, zero on failure.
 */
YBC_API int ybc_set_txn_begin(struct ybc *cache, struct ybc_set_txn *txn,
    const struct ybc_key *key, size_t value_size, uint64_t ttl);

/*
 * Updates value size for the given 'set' transaction.
 *
 * New value size mustn't exceed value_size passed to ybc_set_txn_begin().
 *
 * This function may be called only on active transactions, i.e.
 * before ybc_set_txn_commit*() or ybc_set_txn_rollback() calls.
 */
YBC_API void ybc_set_txn_update_value_size(struct ybc_set_txn *const txn,
    const size_t value_size);

/*
 * Commits the given 'set' transaction.
 *
 * The corresponding item instantly appears in the cache after the commit.
 * The cache doesn't guarantee that the stored item will be available
 * until its' ttl expiration. The item can be evicted from the cache
 * at any time, but in most cases the item will remain available until
 * its' ttl expiration.
 *
 * Use ybc_set_txn_commit_item() instead if you need reading item's contents
 * immediately after the commit.
 * ybc_set_txn_commit_item() is better and faster alternative
 * to ybc_set_txn_commit() + ybc_item_get().
 */
YBC_API void ybc_set_txn_commit(struct ybc_set_txn *txn);

/*
 * Commits the given 'set' transaction.
 *
 * The allocated space for item's value must be populated with contents
 * before commiting the transaction. See ybc_set_txn_get_value()
 * for details.
 *
 * The corresponding item instantly appears in the cache after the commit.
 * The cache doesn't guarantee that the stored item will be available
 * until its' ttl expiration. The item can be evicted from the cache
 * at any time, but in most cases the item will remain available until
 * its' ttl expiration.
 *
 * The function also acquires commited item. The acquired item must be released
 * via ybc_item_release().
 *
 * Use ybc_set_txn_commit() if you don't need reading item's contents
 * immediately after the commit.
 * ybc_set_txn_commit() is better and faster alternative
 * to ybc_set_txn_commit_item() + ybc_item_release().
 */
YBC_API void ybc_set_txn_commit_item(struct ybc_set_txn *txn,
    struct ybc_item *item);

/*
 * Rolls back the given 'set' transaction.
 */
YBC_API void ybc_set_txn_rollback(struct ybc_set_txn *txn);

/*
 * Populates value with a pointer to allocated space and a size of the value.
 *
 * The caller must fill the given space with value.size bytes of item's value
 * before calling ybc_set_txn_commit*().
 *
 * DO NOT write to the allocated space returned by this function after
 * the corresponding transaction is commited or rolled back!
 *
 * Always returns non-NULL value.
 */
YBC_API void ybc_set_txn_get_value(const struct ybc_set_txn *txn,
    struct ybc_set_txn_value *value);


/*******************************************************************************
 * Cache API.
 *
 *
 * Usage:
 *
 * struct ybc_key key = {
 *     .ptr = key_ptr,
 *     .size = key_size,
 * };
 * char item_buf[ybc_item_get_size()];
 * struct ybc_item *const item = (struct ybc_item *)item_buf;
 * struct ybc_value value;
 * size_t item_size;
 * ...
 * if (!ybc_item_get(cache, item, &key)) {
 *   // The value is missing in the cache.
 *   // Build new value (i.e. obtain it from backends, prepare, serialize, etc.)
 *   // and insert it into the cache.
 *   build_new_value(&value);
 *   if (!ybc_item_set_item(cache, item, &key, &value)) {
 *     log_error("Cannot store item to the cache");
 *     exit(EXIT_FAILURE);
 *   }
 * }
 *
 * ybc_item_get_value(item, &value);
 *
 * // The value obtained from ybc_item can be used only until the item
 * // is released via ybc_item_release().
 * use_value(&value);
 *
 * // Always release acquired item after the value no longer needed.
 * ybc_item_release(item);
 ******************************************************************************/

/*
 * Value, which is stored in the cache.
 */
struct ybc_value
{
  /*
   * A pointer to value.
   */
  const void *ptr;

  /*
   * Value size in bytes.
   */
  size_t size;

  /*
   * Remaining time to live in milliseconds.
   */
  uint64_t ttl;
};

/*
 * Status returned by ybc_item_get_de*().
 */
enum ybc_de_status
{
  YBC_DE_NOTFOUND,
  YBC_DE_SUCCESS,
  YBC_DE_WOULDBLOCK,
};

/*
 * Returns the size of ybc_item structure in bytes.
 *
 * The caller is responsible for allocating this amount of memory
 * for the structure before passing it into ybc_item_*() functions.
 *
 * Since the size of ybc_item structure never exceeds a few hundred bytes,
 * it is usually safe allocating space for the structure on the stack.
 */
YBC_API size_t ybc_item_get_size(void);

/*
 * Stores the given value with the given key in the cache.
 *
 * Both key and value are copied from the provided memory locations,
 * so the caller can freely modify memory under the key and the value after
 * returning from the function.
 *
 * Set value->ttl to YBC_MAX_TTL for items without expiration time.
 *
 * The cache doesn't guarantee that the stored item will be available
 * until its' ttl expiration. The item can be evicted from the cache
 * at any time, but in most cases the item will remain available until
 * its' ttl expiration.
 *
 * Returns non-zero on success, zero on error.
 *
 * The function overwrites the pervious value for the given key.
 *
 * Use ybc_item_set_item() instead of ybc_item_set() + ybc_item_get()
 * if you need reading item's contents immediately after storing the item
 * in the cache.
 */
YBC_API int ybc_item_set(struct ybc *cache, const struct ybc_key *key,
    const struct ybc_value *value);

/*
 * Stores the given value with the given key in the cache.
 *
 * Both key and value are copied from the provided memory locations,
 * so the caller can freely modify memory under the key and the value after
 * returning from the function.
 *
 * Set value->ttl to YBC_MAX_TTL for items without expiration time.
 *
 * The cache doesn't guarantee that the stored item will be available
 * until its' ttl expiration. The item can be evicted from the cache
 * at any time, but in most cases the item will remain available until
 * its' ttl expiration.
 *
 * Returns non-zero on success, zero on error.
 *
 * The function overwrites the pervious value for the given key.
 *
 * The returned item MUST be released via ybc_item_release() call.
 *
 * Use ybc_item_set() instead of ybc_item_set_item() + ybc_item_release()
 * if you don't need reading item's contents immediately after storing the item
 * in the cache.
 */
YBC_API int ybc_item_set_item(struct ybc *cache, struct ybc_item *item,
    const struct ybc_key *key, const struct ybc_value *value);

/*
 * Removes an item with the given key from the cache.
 *
 * Returns zero if the item wasn't in the cache, otherwise returns non-zero.
 */
YBC_API int ybc_item_remove(struct ybc *cache, const struct ybc_key *key);

/*
 * Acquires an item with the given key.
 *
 * Returns non-zero on success.
 * Returns zero if an item with the given key isn't found.
 *
 * Item's value can be obtained via ybc_item_get_value() call.
 *
 * Acquired items MUST be released via ybc_item_release() call.
 */
YBC_API int ybc_item_get(struct ybc *cache, struct ybc_item *item,
    const struct ybc_key *key);

/*
 * Acquires an item with automatic dogpile effect (de) handling.
 *
 * Dogpile effect is a race condition when multiple execution threads are
 * simultaneously obtaining or creating the same value in order to insert
 * it into the cache under the same key. Only the last thread 'wins' the race -
 * i.e. its' value will overwrite all the previous values for the given key.
 * All other threads simply waste resources when creating values.
 *
 * Dogpile effect usually occurs when frequently requested item is missing
 * in the cache or approaches its' expiration time in the cache.
 *
 * The dogpile effect may result in huge resource waste if item's construction
 * is resource-expensive.
 *
 * The function automatically suspends all the threads, which are requesting
 * missing value under the same key in the cache, except the first thread,
 * which will receive YBC_DE_NOTFOUND status. It is expected that
 * somebody (not necessarily the first thread) will eventually add missing
 * item, so blocked threads will be eventually resumed. If the value isn't
 * added during grace_ttl period of time, then one of suspended threads
 * is resumed with YBC_DE_NOTFOUND status, while other threads remain
 * suspended, and so on until threads are exhasuted or value is successfully
 * added into the cache.
 *
 * When item's ttl becomes smaller than grace_ttl, then the function may notify
 * a thread (by returning YBC_DE_NOTFOUND), so it can build fresh value
 * for the item, while other threads will receive not-yet-expired value
 * until the item is refreshed or expired.
 *
 * If YBC_DE_NOTFOUND is returned, then it is expected that an item with
 * the given key will be added or refreshed during grace_ttl period of time.
 * The item may be added or refreshed by arbitrary thread, not necessarily
 * the thread, which called ybc_item_get_be().
 *
 * Grace_ttl is set in milliseconds. It shouldn't exceed few minutes - this time
 * should be enough for fetching and constructing new blob from the slowest
 * possible backend on the slowest possible computer.
 * Grace_ttl should be greater than zero milliseconds.
 * Typical grace_ttl should be in the 10ms - 1s range.
 *
 * This function introduces additional overhead comparing to ybc_item_get(),
 * so use it only for items with high probability of dogpile effect.
 *
 * Returns YBC_DE_SUCCESS on success.
 * Returns YBC_DE_NOTFOUND if an item with the given key isn't found.
 *
 * Item's value can be obtained via ybc_item_get_value() call.
 *
 * Acquired items MUST be released with ybc_item_release().
 *
 * Apps based on event loop and/or cooperative multitasking must use
 * ybc_item_get_de_async() instead of this function.
 */
YBC_API enum ybc_de_status ybc_item_get_de(struct ybc *cache,
    struct ybc_item *item, const struct ybc_key *key, uint64_t grace_ttl);

/*
 * This function is almost equivalent to ybc_item_get_de(), except that it
 * never blocks. It returns YBC_DE_WOULDBLOCK if the item isn't ready yet,
 * so the caller can wait for a small amount of time before re-trying to obtain
 * the item again.
 *
 * This function is intended for apps based on event loop and/or cooperative
 * multitasking.
 */
YBC_API enum ybc_de_status ybc_item_get_de_async(struct ybc *cache,
    struct ybc_item *item, const struct ybc_key *key, uint64_t grace_ttl);

/*
 * Releases acquired item.
 *
 * The value returned by ybc_item_get_value() MUST not be used after the item
 * is released.
 * The item MUST not be passed to ybc_item_get_value() after it is released.
 */
YBC_API void ybc_item_release(struct ybc_item *item);

/*
 * Returns a value for the given item.
 *
 * The item must be acquired while calling this function!
 *
 * The returned value may be corrupted if the backing data file is corrupted.
 *
 * If you are unsure in backing data file correctness, then store a checksum
 * alongside item's value and verify it during each item access.
 *
 * The cache doesn't verify value correctess by itself due to performance
 * reasons - checksum calculation on item addition and checksum verification
 * on every item access may require a lot of CPU and memory bandwidth.
 * This is especially true for large blobs (aka multi-GB media files).
 */
YBC_API void ybc_item_get_value(const struct ybc_item *item,
    struct ybc_value *value);


/*******************************************************************************
 * Cache cluster API.
 *
 * Shards requests among available caches proportional to their max_items_count
 * values. Multiple caches can be useful in the following cases:
 * - As a workaround for filesystem limit on maximum file size.
 * - For speeding up I/O-bound cache requests if distinct caches are placed onto
 *   distinct physical devices. Requests can become I/O-bound only if frequently
 *   accessed items don't fit available physical RAM (in other words, program's
 *   working set doesn't fit physical RAM). If program's working set is smaller
 *   than RAM, then there is no any sense in splitting the cache into distinct
 *   shards irregardless of the total cache size (it may be 1000x larger
 *   than physical RAM size, but it should contain 99.9% of rarely accessed
 *   items - aka 'cold items').
 *
 *
 * Usage:
 *
 * // Initialize configs
 * const size_t caches_count = 2;
 * char configs_buf[ybc_config_get_size() * caches_count];
 * struct ybc_config *const configs = (struct ybc_config *)configs_buf;
 * struct ybc_config *config;
 *
 * config = YBC_CONFIG_GET(configs, 0);
 * ybc_config_init(config);
 * ybc_config_set_max_items_count(config, 100 * 1000 * 1000);
 * ybc_config_set_data_file_size(config, (size_t)128 * 1024 * 1024 * 1024);
 * ybc_config_set_index_file(config, "/hdd0/cache.index");
 * ybc_config_set_data_file(config, "/ssd0/cache.data");
 *
 * config = YBC_CONFIG_GET(configs, 1);
 * ybc_config_init(config);
 * ybc_config_set_max_items_count(config, 200 * 1000 * 1000);
 * ybc_config_set_data_file_size(config, (size_t)256 * 1024 * 1024 * 1024);
 * ybc_config_set_index_file(config, "/hdd1/cache.index");
 * ybc_config_set_data_file(config, "/ssd1/cache.data");
 *
 * ...
 *
 * // Open cache cluster
 * char ybc_cluster_buf[ybc_cluster_get_size(caches_count)];
 * struct ybc_cluster *const cluster = (struct ybc_cluster *)ybc_cluster_buf;
 *
 * if (!ybc_cluster_open(cluster, configs, caches_count, 1)) {
 *   log_error("Error when opening cache cluster");
 *   exit(EXIT_FAILURE);
 * }
 *
 * // Destroy configs, since they are no longer needed.
 * ybc_config_destroy(YBC_CONFIG_GET(configs, 0));
 * ybc_config_destroy(YBC_CONFIG_GET(configs, 1));
 *
 * ...
 *
 * // Select appropriate cache for the given key.
 * struct ybc_key key = {
 *     .ptr = key_ptr,
 *     .size = key_size,
 * };
 * char item_buf[ybc_item_get_size()];
 * struct ybc_item *const item = (struct ybc_item *)item_buf;
 * ...
 * struct ybc *const cache = ybc_cluster_get_cache(cluster, &key);
 * ybc_item_get(cache, item, &key);
 * ...
 * ybc_item_release(item);
 * ...
 * ybc_cluster_close(cluster);
 ******************************************************************************/

/*
 * Returns a pointer to the config with the given index in the given
 * array of configs.
 */
#define YBC_CONFIG_GET(configs, index)  \
    ((struct ybc_config *)((char *)(configs) + (index) * ybc_config_get_size()))

/*
 * Cache cluster handler.
 */
struct ybc_cluster;

/*
 * Returns the size of ybc_cluster structure in bytes.
 *
 * The caller is responsible for allocating this amount of memory
 * for the structure before passing it into ybc_cluster_*() functions.
 */
YBC_API size_t ybc_cluster_get_size(size_t caches_count);

/*
 * Opens all the caches_count caches defined in configs.
 *
 * If force is set, then tries fixing errors such as absense
 * of the corresponding files.
 *
 * Returns non-zero on success, zero on failure.
 */
YBC_API int ybc_cluster_open(struct ybc_cluster *cluster,
    const struct ybc_config *configs, size_t caches_count, int force);

/*
 * Closes the given cache cluster.
 */
YBC_API void ybc_cluster_close(struct ybc_cluster *cluster);

/*
 * Returns cache responsible for the corresponding key.
 *
 * Always returns non-NULL result.
 */
YBC_API struct ybc *ybc_cluster_get_cache(struct ybc_cluster *cluster,
    const struct ybc_key *key);

/*
 * Discards all the items in the given cache cluster.
 *
 * This function is very fast and its' speed doesn't depend on the number
 * and the size of items stored in the cluster.
 */
YBC_API void ybc_cluster_clear(struct ybc_cluster *cluster);


/*******************************************************************************
 * Simple API.
 *
 * Use this API when working with small items (up to 1Kb each) and when
 * overwrite protection is disabled (see
 * ybc_config_disable_overwrite_protection() for details).
 *
 * Prefer using ybc_item_*() and ybc_set_txn_*() functions when working with
 * big items, since ybc_simple_*() functions have too high overhead
 * for large items.
 ******************************************************************************/

/*
 * Stores the given (key, value) tuple into the cache.
 *
 * Returns non-zero value on success, zero on failure.
 *
 * Values stored via ybc_simple_set() must be obtained only
 * via ybc_simple_get().
 *
 * Use ybc_simple_*() functions on caches with disabled items' overwrite
 * protection (see ybc_config_disable_overwrite_protection() for details).
  */
YBC_API int ybc_simple_set(struct ybc *cache, const struct ybc_key *key,
const struct ybc_value *value);

/*
 * Fetches value for the given key from the cache.
 *
 * This function is safe to use when overwrite protection is disabled, because
 * it checks value's correctness before returning it.
 *
 * ybc_simple_get() can read only items stored via ybc_simple_set(). It will
 * return 0 on items stored via other interfaces.
 *
 * The caller should initialize value->ptr and value->size before calling
 * this function:
 *   * value->ptr should point to a buffer where item's value should be stored.
 *   * value->size should contain buffer size pointed by value->ptr.
 *
 * Returns:
 *   * 1 on success.
 *   * 0 on cache miss.
 *   * -1 if the items' size is bigger than value->size. In this case
 *     value->size is set to real item's value. It is expected that the caller
 *     will provide enough space for the item in value->ptr and call
 *     ybc_simple_get() again. For instance:
 *
 *        int rv;
 *        value.size = 10;
 *        value.ptr = malloc(value.size);
 *        for (;;) {
 *          rv = ybc_simple_get(cache, key, &value);
 *          if (rv != -1) {
 *            break;
 *          }
 *          value.ptr = realloc(value.ptr, value.size);
 *        }
 *        if (rv == 0) {
 *          handle_cache_miss();
 *        } else {
 *          handle_cache_hit(value);
 *        }
 *        free(value.ptr);
 *
 * Use ybc_simple_*() functions on caches with disabled items' overwrite
 * protection (see ybc_config_disable_overwrite_protection() for details).
 */
YBC_API int ybc_simple_get(struct ybc *const cache,
    const struct ybc_key *const key, struct ybc_value *const value);


#ifdef __cplusplus
}
#endif

#endif  /* YBC_H_INCLUDED */
