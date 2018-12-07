#ifndef YBC_CONFIG_H_INCLUDED
#define YBC_CONFIG_H_INCLUDED

/*******************************************************************************
 * Compile-time configuration settings for ybc.c
 * This file must be included after including platform.h!
 *
 * Naming conventions:
 * - All constants and macros defined here must start with C_.
 ******************************************************************************/

#define C_CONFIG_DEFAULT_MAP_SLOTS_COUNT (100 * 1000)

#define C_CONFIG_DEFAULT_DATA_SIZE (10 * 1024 * 1024)

#define C_CONFIG_DEFAULT_MAP_CACHE_SLOTS_COUNT (10 * 1000)

#define C_CONFIG_DEFAULT_HOT_DATA_SIZE (1024 * 1024)

#define C_CONFIG_DEFAULT_DE_HASHTABLE_SIZE 16

#define C_CONFIG_MAX_DE_HASHTABLE_SIZE (1024 * 1024)

#define C_CONFIG_DEFAULT_SYNC_INTERVAL (10 * 1000)

/*
 * The height of a skiplist embedded into ybc_item.
 *
 * Optimal skiplist height can be calculated as log2(N), where N is the expected
 * maximum number of simultaneously acquired items in your application.
 *
 * Too low height may result in performance degradation when adding items
 * to the cache with high number of simultaneously acquired items.
 *
 * Too high height may waste memory via useless increase of ybc_item size
 * and incur CPU overhead in applications with low number of simultaneously
 * acquired items.
 */
#define C_ITEM_SKIPLIST_HEIGHT 5

/*
 * The map is split into buckets each with size C_MAP_BUCKET_SIZE.
 * The size of bucket containing key digests is:
 *   (C_MAP_BUCKET_SIZE * sizeof(struct m_key_digest))
 * It must be aligned (and probably fit) to CPU cache line for high performance,
 * because all keys in a bucket may be accessed during each lookup operation.
 *
 * Cache's eviction rate depends on the number of slots per bucket.
 * For instance, 16 slots per bucket result in ~0.9% eviction rate for half-full
 * cache - quite cool number ;).
 * See tests/eviction_rate_estimator.py for details.
 * C_MAP_BUCKET_SIZE must be a power of 2.
 */
#define C_MAP_BUCKET_SIZE 16

/*
 * Optimal fill ratio for map slots.
 *
 * This number must be in the range (0.0 .. 1.0].
 * 1.0 means the map performs best when all slots are full.
 * 0.1 means the map performs best when only 10% of slots are full.
 *
 * 40% fill ratio results in 0.1% eviction rate for C_MAP_BUCKET_SIZE = 16.
 * See tests/eviction_rate_estimator.py for details.
 */
#define C_MAP_OPTIMAL_FILL_RATIO 0.4

/*
 * Minimum size of storage file in bytes.
 */
#define C_STORAGE_MIN_SIZE 4096

/*
 * Items with sizes larger than the given value aren't defragmented.
 *
 * There is no reason in moving large items, which span across many VM pages.
 */
#define C_WS_MAX_MOVABLE_ITEM_SIZE (64 * 1024)

/*
 * The probability of item defragmentation if it lays outside of hot data space.
 *
 * The probability must be in the range [0..99].
 * 0 disables the defragmentation, while 99 leads to aggressive defragmentation.
 *
 * Lower probability results in smaller number of m_ws_defragment() calls
 * at the cost of probably higher hot data fragmentation.
 */
#define C_WS_DEFRAGMENT_PROBABILITY 10

/*
 * Minimum grace ttl in milliseconds, which can be passed to ybc_item_get_de().
 *
 * There is no sense in grace ttl, which is smaller than 10 milliseconds.
 */
#define C_DE_ITEM_MIN_GRACE_TTL 10

/*
 * Maximum grace ttl in milliseconds, which can be passed to ybc_item_get_de().
 *
 * Too low maximum grace ttl won't prevent from dogpile effect, when multiple
 * threads are busy with creation of the same item.
 *
 * Too high maximum grace ttl may result in too large m_de->pending_items
 * hashtable if many distinct items are requested via ybc_item_get_de()
 * with high grace ttls. So the maximum grace ttl effectively limits the size
 * of m_de->pending_items hashtable.
 *
 * 10 minutes should be enough for any practical purposes ;)
 */
#define C_DE_ITEM_MAX_GRACE_TTL (10 * 60 * 1000)

/*
 * Time to sleep in milliseconds before the next try on obtaining
 * not-yet-existing item.
 *
 * The amount of sleep time should be enough for creating an average item,
 * which is subject to dogpile effect.
 *
 * Too low sleep time may result in many unsuccessful tries on obtaining
 * not-yet-existing item, i.e. CPU time waste.
 *
 * Too high sleep time may result in high ybc_item_get_de() latencies.
 */
#define C_DE_ITEM_SLEEP_TIME 100

#define C_CLUSTER_INITIAL_HASH_SEED 0xDEADBEEFDEADBEEF

#endif  /* YBC_CONFIG_H_INCLUDED */
