#ifndef YBC_H_INCLUDED
#define YBC_H_INCLUDED

#include <stddef.h>  /* for size_t */
#include <stdint.h>  /* for uint*_t */


#ifdef __cplusplus
extern "C" {
#endif

#if defined(YBC_BUILD_LIBRARY) || defined(YBC_USE_LIBRARY)
#define YBC_API  __attribute__((externally_visible))
#else
#define YBC_API  /* No special handling for static linkage. */
#endif

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
 * ybc_config_set_data_file_size(config, 8 * 1024 * 1024 * 1024);
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
 * Returns size of ybc_config structure in bytes.
 *
 * The caller is responsible for allocating this amount of memory
 * for ybc_config structure before passint it into ybc_config_*() functions.
 */
YBC_API size_t ybc_config_get_size(void);

/*
 * Returns a pointer to the config with the given index.
 */
#define YBC_CONFIG_GET(configs, index)  \
    ((struct ybc_config *)((char *)(configs) + (index) * ybc_config_get_size()))

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
 * This number can be arbitrary large, but in reality it should be equivalent
 * to data file size / avg_item_size, where avg_item_size is the average size
 * of cached item. The size of cached item is roughly equivalent to the size
 * of its' key plus the size of its' value.
 *
 * Index file size will be proportional to this number.
 */
YBC_API void ybc_config_set_max_items_count(struct ybc_config *config,
    size_t max_items_count);

/*
 * Sets data file size in bytes.
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
 * Defragmented index file may lead to faster startup times, when the index
 * is loaded in memory.
 *
 * If index file is not set or set to NULL, then it is automatically created
 * when the cache is opened and automatically deleted when the cache is closed.
 * This effectively disables cache persistence - i.e. the cache
 * will be flushed on the next opening even if data file is non-NULL.
 *
 * It is safe removing the string pointed by filename after the call.
 */
YBC_API void ybc_config_set_index_file(struct ybc_config *config,
    const char *filename);

/*
 * Sets path to the data file for the given config.
 *
 * Data file contains items' values.
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
 * Defragmented data file usually leads to higher cache performance.
 *
 * If data file is not set or set to NULL, then it is automatically created
 * when the cache is opened and automatically deleted when the cache is closed.
 * This effectively disables cache persistence - i.e. the cache
 * will be flushed on the next opening even if index file is non-NULL.
 *
 * It is safe removing the string pointed by filename after the call.
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
 * The optimal number should be slightly higher than the actual number
 * of hot items in the cache.
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
 * This may be useful if the size of hot data is close to the data file size
 * set via ybc_config_set_data_file_size().
 *
 * By default hot data compaction is enabled.
 *
 * Default value should work well for almost all cases, so tune this value only
 * if you know what you are doing.
 */

YBC_API void ybc_config_set_hot_data_size(struct ybc_config *config,
    size_t hot_data_size);

/*
 * Sets interval for cache data syncing in milliseconds.
 *
 * Cache items are periodically synced in data file with this interval.
 * Non-synced cache items may be lost after the program crash or restart.
 *
 * While short sync interval reduces the number of lost items in the event
 * of program crash or exit, it also increases the number of writes to data
 * file, which may slow down the program.
 *
 * Long sync interval minimizes the number of writes to data file at the cost
 * of potentially higher number of lost items in the event of program crash.
 *
 * Setting sync interval to 0 completely disables data syncing.
 * By default syncing is enabled.
 *
 * Default value should work well for almost all cases, so tune this value only
 * if you know what you are doing.
 */
YBC_API void ybc_config_set_sync_interval(struct ybc_config *config,
    uint64_t sync_interval);


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
 * Returns size of ybc structure in bytes.
 *
 * The caller is responsible for allocating this amount of memory
 * for ybc structure before passing it into ybc_*() functions.
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
 * because it may pre-allocates and initialize cache files.
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
 * Config must be non-NULL.
 */
YBC_API void ybc_remove(const struct ybc_config *config);


/*******************************************************************************
 * 'Add' transaction API.
 *
 * The API allows value serialization directly to cache instead of serialization
 * to a temporary buffer before copying serialized contents into cache.
 *
 * All API functions are thread-safe, so there is no need in additional locking.
 *
 *
 * Usage:
 *
 * const struct ybc_key key = {
 *     .ptr = key_ptr,
 *     .size = key_size,
 * };
 * char add_txn_buf[ybc_add_txn_get_size()];
 * struct ybc_add_txn *const txn = (struct ybc_add_txn *)add_txn_buf;
 * char item_buf[ybc_item_get_size()];
 * struct ybc_item *const item = (struct ybc_item *)item_buf;
 *
 * ...
 *
 * value_size = get_serialized_object_size(obj);
 * if (ybc_add_txn_begin(cache, txn, item, &key, value_size)) {
 *   void *value_ptr = ybc_add_txn_get_value_ptr(txn);
 *   if (serialize_object(obj, value_ptr)) {
 *     ybc_add_txn_commit(txn, ttl);
 *
 *     struct ybc_value value;
 *     ybc_item_get_value(item, &value);
 *
 *     use_value(&value);
 *   }
 *
 *   ybc_item_release(item);
 * }
 *
 ******************************************************************************/

/*
 * Acquired item handler.
 */
struct ybc_item;

/*
 * 'Add' transaction handler.
 */
struct ybc_add_txn;

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
 * Returns size of ybc_add_txn structure in bytes.
 *
 * The caller is responsible for allocating this amount of memory
 * for ybc_add_txn structure before passing it into ybc_add_txn_*() functions.
 */
YBC_API size_t ybc_add_txn_get_size(void);

/*
 * Starts 'add' transaction for the given key and value of the given size.
 *
 * Allocates space in the cache for storing an item (key + value).
 *
 * The transaction may be closed either by calling ybc_add_txn_commit().
 *
 * Returns non-zero on success, zero on failure.
 *
 * Acquired item must be released via ybc_item_release().
 * The item can be released also without commit. This is equivalent
 * to transaction rollback.
 */
YBC_API int ybc_add_txn_begin(struct ybc *cache, struct ybc_add_txn *txn,
    struct ybc_item *item, const struct ybc_key *key, size_t value_size);

/*
 * Commits the given 'add' transaction.
 *
 * The corresponding item appears in the cache after the commit with the given
 * ttl (time to live) set.
 *
 * The allocated space for item's value must be populated with contents
 * before commiting the transaction. See ybc_add_txn_get_value_ptr()
 * for details.
 *
 * The item passed to ybc_add_txn_begin() must remain acquired during
 * the call to this function.
 */
YBC_API void ybc_add_txn_commit(struct ybc_add_txn *txn, uint64_t ttl);

/*
 * Returns a pointer to allocated space for item's value.
 *
 * The caller must fill the given space with value_size bytes of item's value
 * before calling ybc_add_txn_commit().
 *
 * Always returns non-NULL value.
 */
YBC_API void *ybc_add_txn_get_value_ptr(const struct ybc_add_txn *txn);


/*******************************************************************************
 * Cache API.
 *
 * This API is built on top of 'add' transaction API.
 *
 * All API functions are thread-safe, i.e. no additional locking is required.
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
 * if (!ybc_item_acquire(cache, item, &key)) {
 * if (!ybc_item_get_value(item, &value)) {
 *   // The value is missing in the cache.
 *   // Build new value (i.e. obtain it from backends, prepare, serialize, etc.)
 *   // and insert it into the cache.
 *   build_new_value(&value);
 *   if (!ybc_item_add(cache, item, &key, &value)) {
 *     log_error("Cannot add item to the cache");
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
 * Value, which is stored in cache.
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
 * Returns size of ybc_item structure in bytes.
 *
 * The caller is responsible for allocating this amount of memory
 * for ybc_item structure before passing it into ybc_item_*() functions.
 */
YBC_API size_t ybc_item_get_size(void);

/*
 * Adds the given value with the given key to the cache.
 *
 * Both key and value are copied from the provided memory locations,
 * so the caller can freely modify memory under key and value after
 * returning from the function.
 *
 * The cache doesn't guarantee that the added item will be available
 * until its' ttl expiration. The item can be evicted from the cache
 * at any time, but in most cases the item will remain available until
 * its' ttl expiration.
 *
 * Returns non-zero on success, zero on error.
 *
 * The function overwrites the pervious value for the given key.
 *
 * The returned item MUST be released via ybc_item_release() call.
 */
YBC_API int ybc_item_add(struct ybc *cache, struct ybc_item *item,
    const struct ybc_key *key, const struct ybc_value *value);

/*
 * Removes an item with the given key from the cache.
 *
 * Does nothing if the item wan't in the cache.
 */
YBC_API void ybc_item_remove(struct ybc *cache, const struct ybc_key *key);

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
YBC_API int ybc_item_acquire(struct ybc *cache, struct ybc_item *item,
    const struct ybc_key *key);

/*
 * Acquires an item with automatic dogpile effect (de) handling.
 *
 * Dogpile effect is a race condition when multiple execution threads
 * simultaneously obtain or create the same value in order to insert it into
 * the cache under the same key. Only the last thread 'wins' the race -
 * i.e. its' value will overwrite all the previous values for the given key.
 * All other threads simply waste computing resources when creating values.
 *
 * Dogpile effect usually occurs when frequently requested item is missing
 * in the cache or expires in the cache.
 *
 * The dogpile effect may result in huge resource waste if item's construction
 * is resource-expensive.
 *
 * The function automatically suspends all the threads, which are requesting
 * missing value under the same key in the cache, except the first thread,
 * which will return zero. It is expected that somebody will add missing item
 * while other threads are waiting. If the value isn't added during grace_ttl
 * period of time, then threads are suspended again, except the one thread and
 * so on until threads are exhasuted or value is successfully added
 * into the cache.
 *
 * When item's ttl becomes smaller than grace_ttl, then the function may notify
 * a thread (by returning zero), so it can build fresh value for the item,
 * while returning not-yet-expired value to other threads until
 * the item will be refreshed.
 *
 * If zero is returned, then it is expected that an item with the given key
 * will be added or refreshed during grace_ttl period of time.
 *
 * This function introduces additional overhead comparing to ybc_item_acquire(),
 * so use it only for cache items with high probability of dogpile effect.
 *
 * Returns non-zero on success.
 * Returns zero if an item with the given key isn't found.
 *
 * Item's value can be obtained via ybc_item_get_value() call.
 *
 * Acquired items MUST be released with ybc_item_release().
 */
YBC_API int ybc_item_acquire_de(struct ybc *cache, struct ybc_item *item,
    const struct ybc_key *key, uint64_t grace_ttl);

/*
 * Releases the item acquired by ybc_item_acquire().
 *
 * The underlying value returned by ybc_item_get_value() cannot be used
 * after the item is released.
 * The item MUST not be passed to ybc_item_get_value() after it is released.
 */
YBC_API void ybc_item_release(struct ybc_item *item);

/*
 * Returns a value for the given item.
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
 *   working doesn't fit physical RAM). If program's working set is smaller
 *   than RAM, then there is no any sense in splitting the cache into distinct
 *   shards irregardless of the total cache size (it may be 1000x larger
 *   than physical RAM size, but it should contain 99.9% of rarely accessed
 *   items - 'cold items').
 *
 * All API functions are thread-safe, so no additional locking is required.
 *
 * Usage:
 *
 * // Initialize configs
 * const size_t caches_count = 2;
 * char configs_buf[ybc_config_get_size() * caches_count];
 * struct ybc_config *const configs = (struct ybc_config *)configs_buf;
 *
 * ybc_config_init(YBC_CONFIG_GET(configs_buf, 0));
 * ybc_config_set_max_items_count(&configs[0], 100 * 1000 * 1000);
 * ybc_config_set_data_file_size(&configs[0], (size_t)128 * 1024 * 1024 * 1024);
 * ybc_config_set_index_file(&configs[0], "/hdd0/cache.index");
 * ybc_config_set_data_file(&configs[0], "/ssd0/cache.data");
 *
 * ybc_config_init(YBC_CONFIG_GET(configs_buf, 1));
 * ybc_config_set_max_items_count(&configs[1], 200 * 1000 * 1000);
 * ybc_config_set_data_file_size(&configs[1], (size_t)256 * 1024 * 1024 * 1024);
 * ybc_config_set_index_file(&configs[1], "/hdd1/cache.index");
 * ybc_config_set_data_file(&configs[1], "/ssd1/cache.data");
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
 * ybc_config_destroy(YBC_CONFIG_GET(configs_buf, 0));
 * ybc_config_destroy(YBC_CONFIG_GET(configs_buf, 1));
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
 * ybc_item_acquire(cache, item, &key);
 * ...
 * ybc_item_release(item);
 * ...
 * ybc_cluster_close(cluster);
 ******************************************************************************/

/*
 * Cache cluster handler.
 */
struct ybc_cluster;

/*
 * Returns size of ybc_cluster structure in bytes.
 *
 * The caller is responsible for allocating this amount of memory
 * for ybc_cluster structure before passing it into ybc_cluster_*() functions.
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

#ifdef __cplusplus
}
#endif

#endif  /* YBC_H_INCLUDED */
