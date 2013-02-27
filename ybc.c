#include "platform.h"  /* Platform-specific functions' implementation. */
#include "config.h"    /* Static configuration macros and constants. */
#include "ybc.h"

#include <assert.h>  /* assert */
#include <stddef.h>  /* size_t */
#include <stdint.h>  /* uint*_t */
#include <stdlib.h>  /* rand */
#include <string.h>  /* memcpy, memcmp, memset */


/*******************************************************************************
 * Cache implementation.
 *
 * Naming conventions:
 * - public functions and structures must start with ybc_
 * - public macros and constants must start with YBC_
 * - private functions and structures must start with m_
 * - private macros and constants must start with M_
 * - platform-specific functions and structures must start with p_.
 * - static configuration macros and constants defined in config.h
 *   must start with C_.
 * Such functions and structures must be defined in platform/<platform_name>.c
 * files.
 *
 * Coding rules:
 * - All variables, which are expected to be immutable in the given code block,
 *   MUST be declared as constants! This provides the following benefits:
 *   + It prevents from accidental modification of the given variable.
 *   + It may help dumb compilers with 'constant propagation' optimizations.
 ******************************************************************************/


/*******************************************************************************
 * Aux API.
 ******************************************************************************/

/*
 * Calculates hash for size bytes starting from ptr using
 * the given seed.
 */
static uint64_t m_hash_get(const uint64_t seed, const void *const ptr,
    const size_t size)
{
  /*
   * FNV hash.
   *
   * http://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function .
   *
   * TODO: use Jenkin's hash instead? It could be faster and have better
   * output distribution.
   * See http://en.wikipedia.org/wiki/Jenkins_hash_function .
   */

  const uint64_t offset_basis = 14695981039346656037UL;
  const uint64_t prime = 1099511628211UL;
  const unsigned char *const v = ptr;
  uint64_t hash = offset_basis ^ seed;

  for (size_t i = 0; i < size; ++i) {
    hash *= prime;
    hash ^= v[i];
  }

  return hash;
}


/*******************************************************************************
 * File API.
 ******************************************************************************/

static void m_file_remove_if_exists(const char *const filename)
{
  if (filename != NULL && p_file_exists(filename)) {
    p_file_remove(filename);
  }
}

/*
 * Tries opening the given file with the given size.
 *
 * If force is set, then creates new file with the given size if it doesn't
 * exist. Force also leads to file size adjustment on its' mismatch.
 *
 * If filename is NULL and force is set, then creates an anonymous file,
 * which will be automatically deleted after the file is closed.
 *
 * Returns non-zero on success, zero on failre.
 * Sets is_file_created to 1 if new file has been created (including
 * anonymous file).
 */
static int m_file_open_or_create(struct p_file *const file,
    const char *const filename, const size_t expected_file_size,
    const int force, int *const is_file_created)
{
  size_t actual_file_size;

  *is_file_created = 0;

  if (filename == NULL) {
    if (!force) {
      return 0;
    }
    /*
     * Though we could just dynamically allocate memory
     * with expected_file_size instead of creating an anonymous file,
     * memory mapped from anonymous file is better than dynamically allocated
     * memory due to the following reasons:
     * - The memory backed by anonymous file doesn't increase process'
     *   commit charge ( http://en.wikipedia.org/wiki/Commit_charge ).
     * - Sequential VM pages backed by anonymous file are mapped to sequential
     *   pages in the anonymous file. This eliminates random I/O during
     *   sequential access to the mapped memory. Of course this is true only
     *   if the file isn't fragmented. See p_file_resize_and_preallocate() call,
     *   which is aimed towards reducing file fragmentation.
     */
    p_file_create_anonymous(file);
    *is_file_created = 1;
  }
  else if (!p_file_exists(filename)) {
    if (!force) {
      return 0;
    }

    p_file_create(file, filename);
    *is_file_created = 1;
  }
  else {
    p_file_open(file, filename);
  }

  p_file_get_size(file, &actual_file_size);
  if (actual_file_size != expected_file_size) {
    if (!force) {
      p_file_close(file);
      if (*is_file_created) {
        m_file_remove_if_exists(filename);
        *is_file_created = 0;
      }
      return 0;
    }

    /*
     * Pre-allocate file space at the moment in order to minimize file
     * fragmentation in the future.
     */
    p_file_resize_and_preallocate(file, expected_file_size);
  }

  return 1;
}


/*******************************************************************************
 * Storage API.
 ******************************************************************************/

/*
 * Cursor points to the position in the storage.
 */
struct m_storage_cursor
{
  /*
   * Whenever the offset wraps storage size, the wrap_count is incremented.
   */
  size_t wrap_count;

  /*
   * An offset in the storage for the given cursor.
   */
  size_t offset;
};

/*
 * Payload points to an item stored in the storage.
 */
struct m_storage_payload
{
  /*
   * A pointer to the value in the storage.
   */
  struct m_storage_cursor cursor;

  /* Item's expiration time in milliseconds starting from unix epoch. */
  uint64_t expiration_time;

  /*
   * Size of the item in the storage.
   */
  size_t size;
};

/*
 * Storage for cached items.
 *
 * Storage is a circular buffer. New items are appended in the front
 * of the storage. Items added into the storage become immutable until the next
 * storage wrap.
 *
 * This layout results in high write speeds on both HDDs and SSDs.
 */
struct m_storage
{
  /*
   * A pointer to the next free memory in the storage.
   */
  struct m_storage_cursor next_cursor;

  /*
   * Storage size in bytes.
   */
  size_t size;

  /*
   * A copy of hash seed from index file.
   *
   * Each item contains a copy of hash seed in its' metadata for validation
   * purposes. See m_storage_metadata_check() for details.
   */
  uint64_t hash_seed;

  /*
   * A pointer to the beginning of the storage.
   */
  char *data;
};

/*
 * An item acquired from the cache.
 *
 * Items returned from the cache are wrapped into ybc_item.
 * This wrapping prevents items' content to be overwritten while in use.
 */
struct ybc_item
{
  /*
   * An associated cache handler.
   */
  struct ybc *cache;

  /*
   * Key size for the given item.
   */
  size_t key_size;

  /*
   * All acquired items are organized into doubly linked skiplist with a head
   * at ybc->acquired_items_head and a tail at ybc->acquired_items_tail.
   *
   * The skiplist helps quickly (in O(ln(n)) time, where n is the number
   * of currently acquired items) determining the location for newly added item
   * in the storage, so the item doesn't corrupt currently acquired items.
   */

  /*
   * Pointers to the next and previous items in the skiplist.
   *
   * Pointers to previous items are used for speeding up acquired items'
   * release.
   */
  struct ybc_item *next[C_ITEM_SKIPLIST_HEIGHT];
  struct ybc_item *prev[C_ITEM_SKIPLIST_HEIGHT];

  /*
   * Item's value location.
   */
  struct m_storage_payload payload;

  /*
   * The flag indicating whether this item is being used in set transaction
   * and didn't commited yet.
   */
  int is_set_txn;
};

static void m_item_assert_less_equal(const struct ybc_item *const a,
    const struct ybc_item *const b)
{
  (void)a;
  (void)b;
  assert(a != b);
  assert(a->payload.cursor.offset <= b->payload.cursor.offset);
}

static void m_item_skiplist_init(struct ybc_item *const acquired_items_head,
    struct ybc_item *const acquired_items_tail, const size_t storage_size)
{
  acquired_items_head->payload.cursor.offset = 0;
  acquired_items_head->payload.size = 0;
  acquired_items_tail->payload.cursor.offset = storage_size;
  acquired_items_tail->payload.size = 0;

  for (size_t i = 0; i < C_ITEM_SKIPLIST_HEIGHT; ++i) {
    acquired_items_head->next[i] = acquired_items_tail;
    acquired_items_head->prev[i] = NULL;
    acquired_items_tail->next[i] = NULL;
    acquired_items_tail->prev[i] = acquired_items_head;
  }
}

static void m_item_skiplist_destroy(struct ybc_item *const acquired_items_head,
    struct ybc_item *const acquired_items_tail, const size_t storage_size)
{
  (void)acquired_items_head;
  (void)acquired_items_tail;
  (void)storage_size;

  assert(acquired_items_head->payload.cursor.offset == 0);
  assert(acquired_items_head->payload.size == 0);
  assert(acquired_items_tail->payload.cursor.offset == storage_size);
  assert(acquired_items_tail->payload.size == 0);

  for (size_t i = 0; i < C_ITEM_SKIPLIST_HEIGHT; ++i) {
    assert(acquired_items_head->next[i] == acquired_items_tail);
    assert(acquired_items_head->prev[i] == NULL);
    assert(acquired_items_tail->next[i] == NULL);
    assert(acquired_items_tail->prev[i] == acquired_items_head);
  }
}

static void m_item_skiplist_assert_valid(const struct ybc_item *const item,
    const size_t i)
{
  if (item->next[i] != NULL) {
    assert(item == item->next[i]->prev[i]);
    m_item_assert_less_equal(item, item->next[i]);
  }
  if (item->prev[i] != NULL) {
    assert(item == item->prev[i]->next[i]);
    m_item_assert_less_equal(item->prev[i], item);
  }
}

static void m_item_skiplist_get_prevs(
    struct ybc_item *const acquired_items_head, struct ybc_item **const prevs,
    const size_t offset)
{
  struct ybc_item *prev = acquired_items_head;

  for (size_t i = 0; i < C_ITEM_SKIPLIST_HEIGHT; ++i) {
    assert(prev->payload.cursor.offset <= offset);
    struct ybc_item *next = prev->next[i];
    while (offset > next->payload.cursor.offset) {
      m_item_skiplist_assert_valid(next, i);
      prev = next;
      next = next->next[i];
    }
    m_item_skiplist_assert_valid(next, i);
    prevs[i] = prev;
  }
}

static void m_item_skiplist_add(struct ybc_item *const item) {
  const size_t offset = item->payload.cursor.offset;
  uint64_t h = m_hash_get(0, &offset, sizeof(offset));

  for (size_t i = C_ITEM_SKIPLIST_HEIGHT; i > 0; ) {
    --i;
    struct ybc_item *const prev = item->next[i];
    m_item_assert_less_equal(prev, item);
    m_item_assert_less_equal(item, prev->next[i]);
    item->next[i] = prev->next[i];
    item->prev[i] = prev;
    prev->next[i]->prev[i] = item;
    prev->next[i] = item;

    if (!(h & 1)) {
      while (i > 0) {
        --i;
        item->next[i] = NULL;
        item->prev[i] = NULL;
      }
      break;
    }
    h >>= 1;
  }
}

static void m_item_skiplist_del(struct ybc_item *const item)
{
  for (size_t i = C_ITEM_SKIPLIST_HEIGHT; i > 0; ) {
    --i;
    if (item->next[i] == NULL) {
      break;
    }
    m_item_skiplist_assert_valid(item, i);
    item->prev[i]->next[i] = item->next[i];
    item->next[i]->prev[i] = item->prev[i];
  }
}

static void m_item_skiplist_relocate(struct ybc_item *const dst,
    struct ybc_item *const src)
{
  *dst = *src;

  for (size_t i = C_ITEM_SKIPLIST_HEIGHT; i > 0; ) {
    --i;
    if (src->next[i] == NULL) {
      break;
    }
    m_item_skiplist_assert_valid(src, i);
    src->prev[i]->next[i] = dst;
    src->next[i]->prev[i] = dst;
  }
}

static void m_storage_fix_size(size_t *const size)
{
  if (*size < C_STORAGE_MIN_SIZE) {
    *size = C_STORAGE_MIN_SIZE;
  }
}

static int m_storage_open(struct m_storage *const storage,
    const char *const filename, const int force, int *const is_file_created)
{
  struct p_file file;
  void *ptr;

  if (!m_file_open_or_create(&file, filename, storage->size, force,
      is_file_created)) {
    return 0;
  }

  /*
   * Do not cache storage file contents into RAM at the moment, because it can
   * be much bigger than RAM size. Allow the OS dealing with storage file
   * caching.
   */

  p_memory_map(&ptr, &file, storage->size);
  assert((uintptr_t)storage->size <= UINTPTR_MAX - (uintptr_t)ptr);

  /*
   * It is assumed that memory mapping of the file remains active after the file
   * is closed.
   */
  p_file_close(&file);

  storage->data = ptr;

  /*
   * Do not verify correctness of storage data at the moment due
   * to the following reasons:
   * - It can take a lot of time if storage file is too big.
   * - The cache invalidates items with incorrect keys on the fly.
   * - The cache code is designed in the way, which prevents inconsistencies
   *   in data file under normal mode of operation and even under unexpected
   *   process termination.
   *
   * Library users can verify data correctness by embedding and verifiyng
   * checksums into item's values.
   */

  return 1;
}

static void m_storage_close(struct m_storage *const storage)
{
  p_memory_unmap(storage->data, storage->size);
}

static void *m_storage_get_ptr(const struct m_storage *const storage,
    const size_t offset)
{
  assert(offset <= storage->size);
  assert((uintptr_t)offset <= UINTPTR_MAX - (uintptr_t)storage->data);

  return storage->data + offset;
}

static void m_storage_payload_assert_valid(
    const struct m_storage_payload *const payload, const size_t storage_size)
{
  (void)payload;
  (void)storage_size;

  assert(payload->size <= storage_size);
  assert(payload->cursor.offset <= storage_size);
  assert(payload->cursor.offset <= storage_size - payload->size);
}

static int m_storage_find_free_hole(struct ybc_item *const acquired_items_head,
    struct ybc_item *const item, struct m_storage_cursor *const next_cursor,
    const size_t item_size, const size_t storage_size)
{
  m_item_skiplist_get_prevs(acquired_items_head, item->next,
      next_cursor->offset);
  const size_t N = C_ITEM_SKIPLIST_HEIGHT - 1;
  const struct ybc_item *tmp = item->next[N];
  /*
   * It is expected that item's properties are already verified
   * in ybc_item_get(), so use only assertions here.
   */
  m_storage_payload_assert_valid(&tmp->payload, storage_size);

  if (next_cursor->offset >= tmp->payload.cursor.offset + tmp->payload.size) {
    tmp = tmp->next[N];
    m_storage_payload_assert_valid(&tmp->payload, storage_size);

    assert(next_cursor->offset <= storage_size - item_size);

    if (next_cursor->offset + item_size <= tmp->payload.cursor.offset) {
      return 1;
    }
  }

  next_cursor->offset = tmp->payload.cursor.offset + tmp->payload.size;
  return 0;
}

/*
 * Allocates storage space for the given item with the given item->payload.size.
 *
 * On success returns non-zero, sets up item->cursor to point to the allocated
 * space in the storage.
 * Registers the item in acquired_items skiplist if has_overwrite_protection
 * is set.
 *
 * On failure returns zero.
 */
static int m_storage_allocate(struct m_storage *const storage,
    struct ybc_item *const acquired_items_head, struct ybc_item *const item,
    const int has_overwrite_protection)
{
  const size_t item_size = item->payload.size;
  assert(item_size > 0);

  const size_t storage_size = storage->size;
  if (item_size > storage_size) {
    return 0;
  }

  struct m_storage_cursor next_cursor = storage->next_cursor;
  assert(next_cursor.offset <= storage_size);

  const size_t initial_offset = next_cursor.offset;
  int is_storage_wrapped = 0;
  for (;;) {
    if (next_cursor.offset > storage_size - item_size) {
      /* Hit the end of storage. Wrap the cursor. */
      ++next_cursor.wrap_count;
      next_cursor.offset = 0;
      is_storage_wrapped = 1;
    }

    if (!has_overwrite_protection) {
      break;
    }
    if (m_storage_find_free_hole(acquired_items_head, item, &next_cursor,
        item_size, storage_size)) {
      break;
    }

    if (is_storage_wrapped && next_cursor.offset >= initial_offset) {
      /* Couldn't find a hole with appropriate size in the storage. */
      return 0;
    }
  }

  /*
   * Set up item->payload.cursor and register the item
   * in acquired_items skiplist.
   */
  assert(next_cursor.offset < storage_size);
  item->payload.cursor = next_cursor;

  if (has_overwrite_protection) {
    m_item_skiplist_add(item);
  }

  /* Update storage->next_cursor */
  assert(next_cursor.offset <= storage_size - item_size);
  next_cursor.offset += item_size;
  storage->next_cursor = next_cursor;

  return 1;
}

/*
 * Checks payload correctness.
 *
 * Doesn't check item's key, which is stored in the storage, due to performance
 * reasons (avoids random memory access).
 *
 * See also m_storage_metadata_check().
 *
 * Returns non-zero on successful check, zero on failure.
 */
static int m_storage_payload_check(const struct m_storage *const storage,
    const struct m_storage_cursor *const next_cursor,
    const struct m_storage_payload *const payload, const uint64_t current_time)
{
  size_t max_offset = next_cursor->offset;

  if (payload->expiration_time < current_time) {
    /* The item has been expired. */
    return 0;
  }

  if (payload->cursor.wrap_count != next_cursor->wrap_count) {
    if (payload->cursor.wrap_count != next_cursor->wrap_count - 1) {
      /* The item is oudated or it has invalid wrap_count. */
      return 0;
    }

    if (payload->cursor.offset < next_cursor->offset) {
      /* The item is outdated. */
      return 0;
    }

    max_offset = storage->size;
  }

  if (payload->cursor.offset > max_offset) {
    /* The item has invalid offset. */
    return 0;
  }

  if (payload->size > max_offset - payload->cursor.offset) {
    /* The item has invalid size. */
    return 0;
  }

  return 1;
}

static size_t m_storage_metadata_get_size(const size_t key_size) {
  /*
   * Payload metadata contains the following fields:
   * - digest (key size ^ payload size ^ hash seed)
   * - key data
   */
  static const size_t const_metadata_size = sizeof(size_t);

  assert(key_size <= SIZE_MAX - const_metadata_size);
  return key_size + const_metadata_size;
}

static size_t m_storage_metadata_get_digest(const uint64_t hash_seed,
    const size_t key_size, const size_t payload_size)
{
  return (size_t)hash_seed ^ key_size ^ payload_size;
}

static void m_storage_metadata_save(
    const struct m_storage *const storage,
    const struct m_storage_payload *const payload,
    const struct ybc_key *const key)
{
  const size_t metadata_size = m_storage_metadata_get_size(key->size);
  char *ptr = m_storage_get_ptr(storage, payload->cursor.offset);
  assert(((uintptr_t)ptr) <= UINTPTR_MAX - metadata_size);
  (void)metadata_size;

  const size_t digest = m_storage_metadata_get_digest(storage->hash_seed,
      key->size, payload->size);
  memcpy(ptr, &digest, sizeof(digest));

  ptr += sizeof(digest);
  memcpy(ptr, key->ptr, key->size);
}

static void m_storage_metadata_update_payload_size(
    const struct m_storage *const storage,
    const struct m_storage_payload *const payload,
    const size_t old_payload_size, const size_t key_size)
{
  const size_t metadata_size = m_storage_metadata_get_size(key_size);
  char *ptr = m_storage_get_ptr(storage, payload->cursor.offset);
  assert(((uintptr_t)ptr) <= UINTPTR_MAX - metadata_size);
  (void)metadata_size;

  size_t digest;
  memcpy(&digest, ptr, sizeof(digest));
  digest ^= old_payload_size ^ payload->size;
  memcpy(ptr, &digest, sizeof(digest));
}

/*
 * Checks metadata correctness for an item with the given payload.
 *
 * The function is slower than m_storage_payload_check(), because it accesses
 * random location in the storage. Use this function only if you really need it.
 *
 * Returns non-zero on successful check, zero on failure.
 */
static int m_storage_metadata_check(const struct m_storage *const storage,
    const struct m_storage_payload *const payload,
    const struct ybc_key *const key)
{
  const size_t metadata_size = m_storage_metadata_get_size(key->size);

  if (payload->size < metadata_size) {
    /*
     * Payload's size cannot be smaller than the size of metadata,
     * because payload includes the metadata.
     */
    return 0;
  }

  const char *ptr = m_storage_get_ptr(storage, payload->cursor.offset);
  assert(((uintptr_t)ptr) <= UINTPTR_MAX - metadata_size);

  const size_t digest = m_storage_metadata_get_digest(storage->hash_seed,
      key->size, payload->size);

  if (memcmp(ptr, &digest, sizeof(digest))) {
    /* Invalid digest. */
    return 0;
  }

  ptr += sizeof(digest);
  if (memcmp(ptr, key->ptr, key->size)) {
    /* Invalid key data. */
    return 0;
  }

  return 1;
}


/*******************************************************************************
 * Working set defragmentation API.
 *
 * The defragmentation tries minimizing working set fragmentation by packing
 * frequently accessed items into a contiguous memory area.
 *
 * The defragmentation can reduce working set size for caches containing
 * a lot of items with sizes smaller than VM page size. For instance, a VM page
 * can contain up to 32 items each with 128 bytes size. Suppose each
 * page contains only one frequently requested item. Then the storage
 * effectively wastes 31*128 bytes per VM page. The defragmentation would
 * compact frequently accessed items (aka 'working set') into a contiguous
 * memory area, which will occupy only 1/32s (~3%) of initial VM space.
 *
 * The defragmentation also may prolong life for frequently requested small
 * items. Such items are always maintained in the front of storage, so they
 * won't be deleted on storage space wrap.
 *
 * The defragmentation won't help for caches containing a lot of large items
 * with sizes much larger than VM page size (hundreds of KBs or larger).
 *
 * TODO: automatically adjust estimated working set size in runtime. Currently
 * it can be set only at cache opening.
 ******************************************************************************/

static void m_ws_fix_hot_data_size(size_t *const hot_data_size,
    const size_t storage_size)
{
  if (*hot_data_size > storage_size / 2) {
    *hot_data_size = storage_size / 2;
  }
}

/*
 * Defragments the given item, i.e. moves it into the front of storage's
 * free space.
 *
 * There is a race condition possible when another thread adds new item
 * with the given key before the defragmentation for this item is complete.
 * In this case new item will become overwritten by the old item after
 * the defragmentation is complete. But since this is a cache, not a persistent
 * storage, this should be OK - subsequent readers should notice old value
 * and overwrite it with new value.
 *
 * Since this operation can be quite costly, avoid performing it in hot paths.
 */
static void m_ws_defragment(struct ybc *const cache,
    const struct ybc_item *const item, const struct ybc_key *const key)
{
  struct ybc_value value;

  ybc_item_get_value(item, &value);
  (void)ybc_item_set(cache, key, &value);
}

/*
 * Checks whether the item pointed by the given payload should be defragmented.
 *
 * Returns 1 if the item should be defragmented, i.e. should be moved
 * to the front of the storage. Otherwise returns 0.
 */
static int m_ws_should_defragment(const struct m_storage *const storage,
    const struct m_storage_cursor *const next_cursor,
    const struct m_storage_payload *const payload, const size_t hot_data_size)
{
  if (hot_data_size == 0) {
    /* Defragmentation is disabled. */
    return 0;
  }

  const size_t distance =
      (next_cursor->offset >= payload->cursor.offset) ?
      (next_cursor->offset - payload->cursor.offset) :
      (storage->size - (payload->cursor.offset - next_cursor->offset));

  if (distance < hot_data_size) {
    /* Do not defragment recently added or defragmented items. */
    return 0;
  }

  if (payload->size > C_WS_MAX_MOVABLE_ITEM_SIZE) {
    /* Do not defragment large items. */
    return 0;
  }

  /*
   * It is OK using non-thread-safe and non-reentrant rand() here,
   * since we do not need reproducible sequence of random values.
   */
  if ((rand() % 100) >= C_WS_DEFRAGMENT_PROBABILITY) {
    /*
     * Probabalistically skip items to be defragmented.
     * This way one-off items (i.e. items requested only once) are likely
     * to be skipped, while frequently accessed items will be eventually
     * defragmented.
     */
    return 0;
  }

  return 1;
}


/*******************************************************************************
 * Key digest API.
 *
 * TODO: probably remove this level of abstraction?
 ******************************************************************************/

/*
 * A digest of cache key.
 */
struct m_key_digest
{
  uint64_t digest;
};

static const struct m_key_digest M_KEY_DIGEST_EMPTY = {
    .digest = 0,
};

static int m_key_digest_equal(const struct m_key_digest *const first,
    const struct m_key_digest *const second)
{
  return first->digest == second->digest;
}

static int m_key_digest_is_empty(const struct m_key_digest *const key_digest)
{
  return m_key_digest_equal(key_digest, &M_KEY_DIGEST_EMPTY);
}

static void m_key_digest_clear(struct m_key_digest *const key_digest)
{
  *key_digest = M_KEY_DIGEST_EMPTY;
}

static void m_key_digest_get(struct m_key_digest *const key_digest,
    const uint64_t hash_seed, const struct ybc_key *const key)
{
  key_digest->digest = m_hash_get(hash_seed, key->ptr, key->size);
  if (m_key_digest_is_empty(key_digest)) {
    ++key_digest->digest;
  }
}

static size_t m_key_digest_mod(const struct m_key_digest *const key_digest,
    const size_t n)
{
  assert(!m_key_digest_is_empty(key_digest));
  return key_digest->digest % n;
}


/*******************************************************************************
 * Map API.
 ******************************************************************************/

/*
 * It is expected that C_MAP_BUCKET_SIZE is a power of 2.
 */
static const size_t M_MAP_BUCKET_MASK = C_MAP_BUCKET_SIZE - 1;

/*
 * The size of a compound map item, which consists of key digest
 * and storage payload.
 */
#define M_MAP_ITEM_SIZE (sizeof(struct m_key_digest) + \
    sizeof(struct m_storage_payload))

/*
 * The size of aux data in the map file.
 *
 * Aux data consists of the following items:
 * - m_storage_cursor
 * - hash_seed
 */
#define M_MAP_AUX_DATA_SIZE (sizeof(struct m_storage_cursor) + sizeof(uint64_t))

/*
 * The maximum allowed number of slots in the map.
 */
static const size_t M_MAP_SLOTS_COUNT_LIMIT = (SIZE_MAX - M_MAP_AUX_DATA_SIZE) /
    M_MAP_ITEM_SIZE;

/*
 * Hash map, which maps key digests to cache items from the storage.
 *
 * The code below intentionally omits serialization of concurrent access
 * to key_digests and payloads, i.e. it intentially allows race conditions,
 * which may lead to broken key_digest and/or payload values.
 * Such breakage is expected - it is automatically recovered by clearing broken
 * slots. m_storage_payload_check() and m_storage_metadata_check() are used
 * for detecting broken slots in the map.
 */
struct m_map
{
  /*
   * The number of slots in the map.
   */
  size_t slots_count;

  /*
   * Slots' key digests.
   */
  struct m_key_digest *key_digests;

  /*
   * Slots' payloads.
   */
  struct m_storage_payload *payloads;
};

static void m_map_fix_slots_count(size_t *const slots_count,
    const size_t data_file_size)
{
  /*
   * Preserve the order of conditions below!
   * This guarantees that the resulting slots_count will be between
   * C_MAP_BUCKET_SIZE and M_MAP_SLOTS_COUNT_LIMIT and
   * is divided by C_MAP_BUCKET_SIZE.
   */

  if (*slots_count > data_file_size) {
    /*
     * The number of slots cannot exceed the size of data file, because
     * each slot requires at least one byte in the data file for one-byte key.
     */
    *slots_count = data_file_size;
  }

  if (*slots_count < C_MAP_BUCKET_SIZE) {
    *slots_count = C_MAP_BUCKET_SIZE;
  }

  if (*slots_count > M_MAP_SLOTS_COUNT_LIMIT) {
    *slots_count = M_MAP_SLOTS_COUNT_LIMIT;
  }

  if (*slots_count % C_MAP_BUCKET_SIZE) {
    *slots_count += C_MAP_BUCKET_SIZE - (*slots_count % C_MAP_BUCKET_SIZE);
  }
}

static void m_map_init(struct m_map *const map, const size_t slots_count,
    struct m_key_digest *const key_digests,
    struct m_storage_payload *const payloads)
{
  assert(C_MAP_BUCKET_SIZE == M_MAP_BUCKET_MASK + 1);
  assert((C_MAP_BUCKET_SIZE & M_MAP_BUCKET_MASK) == 0);

  map->slots_count = slots_count;
  map->key_digests = key_digests;
  map->payloads = payloads;
}

static void m_map_destroy(struct m_map *const map)
{
  map->slots_count = 0;
  map->key_digests = NULL;
  map->payloads = NULL;
}

/*
 * Looks up slot index for the given key digest.
 *
 * Sets start_index to the index of the first slot in the bucket,
 * which contains the given key digest.
 *
 * Sets slot_index to the index of the slot containing the given key.
 *
 * Return non-zero on success. Returns 0 if the given key cannot be found
 * in the map.
 */
static int m_map_lookup_slot_index(const struct m_map *const map,
    const struct m_key_digest *const key_digest, size_t *const start_index,
    size_t *const slot_index)
{
  assert(map->slots_count % C_MAP_BUCKET_SIZE == 0);
  assert(map->slots_count >= C_MAP_BUCKET_SIZE);

  *start_index = (m_key_digest_mod(key_digest, map->slots_count) &
      ~M_MAP_BUCKET_MASK);

  for (size_t i = 0; i < C_MAP_BUCKET_SIZE; ++i) {
    const size_t current_index = *start_index + i;
    assert(current_index < map->slots_count);

    if (m_key_digest_equal(&map->key_digests[current_index], key_digest)) {
      *slot_index = current_index;
      return 1;
    }
  }

  return 0;
}

/*
 * Adds the given payload with the given key_digest into the map.
 */
static void m_map_set(const struct m_map *const map,
    const struct m_key_digest *const key_digest,
    const struct m_storage_payload *const payload)
{
  size_t start_index, slot_index;

  if (!m_map_lookup_slot_index(map, key_digest, &start_index, &slot_index)) {
    /* Try occupying the first empty slot in the bucket. */
    size_t i;
    size_t victim_index = start_index;
    uint64_t min_expiration_time = UINT64_MAX;

    for (i = 0; i < C_MAP_BUCKET_SIZE; ++i) {
      slot_index = start_index + i;
      assert(slot_index < map->slots_count);

      const struct m_storage_payload current_payload =
          map->payloads[slot_index];

      if (m_key_digest_is_empty(&map->key_digests[slot_index])) {
        /* Found an empty slot. */
        break;
      }

      /*
       * This code determines 'victim' slot, which will be overwritten
       * in the case all slots in the bucket are occupied.
       *
       * 'victim' slot has minimum expiration time, i.e. it contains an item,
       * which would expire first in the given bucket. We just 'accelerate' its'
       * expiration if all slots in the bucket are occupied.
       */
      if (current_payload.expiration_time < min_expiration_time) {
        min_expiration_time = current_payload.expiration_time;
        victim_index = slot_index;
      }
    }

    if (i == C_MAP_BUCKET_SIZE) {
      /*
       * Couldn't find empty slot.
       * Overwrite slot, which will expire sooner than other slots.
       */
      slot_index = victim_index;
    }

    map->key_digests[slot_index] = *key_digest;
  }
  map->payloads[slot_index] = *payload;
}

static int m_map_remove(const struct m_map *const map,
    const struct m_key_digest *const key_digest)
{
  size_t start_index, slot_index;

  if (m_map_lookup_slot_index(map, key_digest, &start_index, &slot_index)) {
    m_key_digest_clear(&map->key_digests[slot_index]);
    return 1;
  }
  return 0;
}

/*
 * Obtains a payload with the given key_digest in the map.
 *
 * Returns 1 if a payload with the given key_digest has been found in the map.
 * Returns 0 otherwise.
 */
static int m_map_get(const struct m_map *const map,
    const struct m_key_digest *const key_digest,
    struct m_storage_payload *const payload)
{
  size_t start_index_unused, slot_index;

  if (!m_map_lookup_slot_index(map, key_digest, &start_index_unused,
      &slot_index)) {
    return 0;
  }
  *payload = map->payloads[slot_index];

  return 1;
}

/*******************************************************************************
 * Map cache API.
 *
 * TODO: automatically adjust map cache size in runtime. Currently it can be set
 * only at cache opening.
 ******************************************************************************/

static void m_map_cache_fix_slots_count(size_t *const slots_count,
    const size_t map_slots_count)
{
  if (*slots_count == 0) {
    /* Map cache is disabled. */
    return;
  }

  if (*slots_count > map_slots_count / 2) {
    *slots_count = map_slots_count / 2;
  }

  m_map_fix_slots_count(slots_count, SIZE_MAX);
}

static void m_map_cache_init(struct m_map *const map_cache,
    const size_t slots_count)
{
  if (slots_count == 0) {
    /* Map cache is disabled. */
    m_map_init(map_cache, 0, NULL, NULL);
    return;
  }

  assert(slots_count <= SIZE_MAX / M_MAP_ITEM_SIZE);
  const size_t map_cache_size = slots_count * M_MAP_ITEM_SIZE;

  struct m_key_digest *const key_digests = p_malloc(map_cache_size);
  struct m_storage_payload *const payloads = (struct m_storage_payload *)
      (key_digests + slots_count);

  /*
   * Though the code can gracefully handle invalid key digests and payloads,
   * let's fill them with zeroes. This has the following benefits:
   * - It will shut up valgrind regarding comparison with uninitialized memory.
   * - It will force the OS attaching physical RAM to the allocated region
   *   of memory. This eliminates possible minor pagefaults during the cache
   *   warm-up ( http://en.wikipedia.org/wiki/Page_fault#Minor ).
   */
  memset(key_digests, 0, map_cache_size);

  m_map_init(map_cache, slots_count, key_digests, payloads);
}

static void m_map_cache_destroy(struct m_map *const map_cache)
{
  p_free(map_cache->key_digests);
  m_map_destroy(map_cache);
}

/*
 * Obtains a payload for the given key_digest in the map_cache and/or map.
 *
 * Returns 1 if a payload with the given key_digest has been found
 * in the map_cache and/or map. Otherwise returns 0.
 */
static int m_map_cache_get(const struct m_map *const map,
    const struct m_map *const map_cache,
    const struct m_key_digest *const key_digest,
    struct m_storage_payload *const payload)
{
  if (map_cache->slots_count == 0) {
    /* The map cache is disabled. Look up the item via map. */
    return m_map_get(map, key_digest, payload);
  }

  /*
   * Fast path: look up the item via the map cache.
   */
  if (m_map_get(map_cache, key_digest, payload)) {
    return 1;
  }

  /*
   * Slow path: fall back to looking up the item via map.
   */
  if (!m_map_get(map, key_digest, payload)) {
    return 0;
  }

  /*
   * Add the found item to the map cache.
   */
  m_map_set(map_cache, key_digest, payload);
  return 1;
}

/*
 * Adds the given payload with the given key_digest into the map_cache and map.
 */
static void m_map_cache_set(const struct m_map *const map,
    const struct m_map *const map_cache,
    const struct m_key_digest *const key_digest,
    const struct m_storage_payload *const payload)
{
  if (map_cache->slots_count != 0) {
    (void)m_map_remove(map_cache, key_digest);
  }
  m_map_set(map, key_digest, payload);
}

static int m_map_cache_remove(const struct m_map *const map,
    const struct m_map *const map_cache,
    const struct m_key_digest *const key_digest)
{
  if (map_cache->slots_count != 0) {
    (void)m_map_remove(map_cache, key_digest);
  }
  return m_map_remove(map, key_digest);
}


/*******************************************************************************
 * Index API.
 ******************************************************************************/

/*
 * Cache index.
 */
struct m_index
{
  /*
   * A mapping between cache items and values.
   *
   * Map's data is mapped directly into index file.
   */
  struct m_map map;

  /*
   * A cache, which contains frequently accessed items from the map.
   *
   * Since an item stored in the map occupies a tiny fraction of VM page size
   * (currently 1/100th), each VM page behind the map is likely to contain only
   * a few frequently accessed items. The cache allows packing such items more
   * tightly, thus reducing the number of frequently accessed VM pages
   * (aka 'working set').
   */
  struct m_map map_cache;

  /*
   * A pointer to hash seed.
   *
   * This pointer points to the corresponding location in index file.
   *
   * Hash seed is used for key digest calculations. It serves the following
   * purposes:
   * - Security. It reduces chances for successful hash table collision attack.
   *   (Though this attack is harmless for the current m_map implementation)
   * - Fast cache data invalidation. See ybc_clear().
   */
  uint64_t *hash_seed_ptr;
};

static size_t m_index_get_file_size(const size_t slots_count)
{
  /*
   * Index file consists of the following items:
   * - aux data.
   * - slots_count items.
   */
  assert(slots_count <= M_MAP_SLOTS_COUNT_LIMIT);

  return slots_count * M_MAP_ITEM_SIZE + M_MAP_AUX_DATA_SIZE;
}

static int m_index_open(struct m_index *const index,
    const size_t map_slots_count, const size_t map_cache_slots_count,
    const char *const filename, const int force, int *const is_file_created,
    struct m_storage_cursor **const next_cursor)
{
  struct p_file file;
  void *ptr;

  const size_t file_size = m_index_get_file_size(map_slots_count);

  if (!m_file_open_or_create(&file, filename, file_size, force,
      is_file_created)) {
    return 0;
  }

  /*
   * Cache index file contents in RAM in order to minimize random I/O during
   * cache warm-up.
   *
   * Since index file is usually not too big, it can be quickly cached into RAM
   * with sustained speed at hundreds MB/s (which is equivalent to millions
   * of key slots per second) on modern HDDs and SSDs. Of course, this is true
   * only if the index file's fragmentation is low. The code tries hard
   * achieving low fragmentation by pre-allocating file contents
   * at creation time.
   * See m_file_open_or_create() for details.
   */
  p_file_cache_in_ram(&file, file_size);

  /*
   * Hint the OS about random access pattern to index file contents.
   */
  p_file_advise_random_access(&file, file_size);

  p_memory_map(&ptr, &file, file_size);
  assert((uintptr_t)file_size <= UINTPTR_MAX - (uintptr_t)ptr);

  /*
   * It is assumed that memory mapping of the file remains active after the file
   * is closed.
   */
  p_file_close(&file);

  /*
   * Key digests must be aligned to CPU cache line size for faster lookups.
   * Assume the ptr is VM page-aligned, so there are high chances it is aligned
   * to CPU cache line size. So, index file must start with key digests.
   *
   * See C_MAP_BUCKET_SIZE description for more details.
   */
  struct m_key_digest *const key_digests = ptr;
  struct m_storage_payload *const payloads = (struct m_storage_payload *)
      (key_digests + map_slots_count);
  m_map_init(&index->map, map_slots_count, key_digests, payloads);

  *next_cursor = (struct m_storage_cursor *)(payloads + map_slots_count);
  index->hash_seed_ptr = (uint64_t *)(*next_cursor + 1);
  if (*is_file_created) {
    *index->hash_seed_ptr = p_get_current_time();
  }

  /*
   * Do not verify correctness of loaded index file now, because the cache
   * discovers and fixes errors in the index file on the fly.
   */

  m_map_cache_init(&index->map_cache, map_cache_slots_count);

  return 1;
}

static void m_index_close(struct m_index *const index)
{
  m_map_cache_destroy(&index->map_cache);

  const size_t file_size = m_index_get_file_size(index->map.slots_count);
  p_memory_unmap(index->map.key_digests, file_size);

  m_map_destroy(&index->map);
}

/*******************************************************************************
 * Sync API.
 *
 * Syncing is performed by a separate thread in order to avoid latency spikes
 * during cache accesses.
 ******************************************************************************/

/*
 * Data related to storage syncing.
 */
struct m_sync
{
  /*
   * Interval between data syncs.
   */
  uint64_t sync_interval;

  /*
   * A copy of ybc->has_overwrite_protection.
   */
  int has_overwrite_protection;

  /*
   * Start pointer for unsynced storage data.
   */
  struct m_storage_cursor *sync_cursor;

  /*
   * A pointer to ybc->storage.
   */
  struct m_storage *storage;

  /*
   * A pointer to ybc->acquired_items_head.
   */
  struct ybc_item *acquired_items_head;

  /*
   * A pointer to ybc->lock.
   */
  struct p_lock *cache_lock;

  /*
   * An event for indicating when the sync_thread should be stopped.
   */
  struct p_event stop_event;

  /*
   * A thread responsible for data syncing.
   */
  struct p_thread sync_thread;
};

/*
 * Adjusts next_cursor, so the range [sync_cursor ... next_cursor)
 * doesn't contain items with 'set_txn' state.
 * There is no sense in syncing items with 'set_txn' state, because their
 * contents is likely to be modified in the near future.
 */
static void m_sync_adjust_next_cursor(
    struct ybc_item *const acquired_items_head,
    const struct m_storage_cursor *const sync_cursor,
    struct m_storage_cursor *const next_cursor)
{
  struct ybc_item *prevs[C_ITEM_SKIPLIST_HEIGHT];
  m_item_skiplist_get_prevs(acquired_items_head, prevs, sync_cursor->offset);

  const size_t N = C_ITEM_SKIPLIST_HEIGHT - 1;
  const struct ybc_item *item = prevs[N]->next[N];
  m_item_skiplist_assert_valid(item, N);
  while (item->payload.cursor.offset < next_cursor->offset) {
    if (item->is_set_txn) {
      *next_cursor = item->payload.cursor;
      break;
    }
    item = item->next[N];
    m_item_skiplist_assert_valid(item, N);
  }
}

static void m_sync_commit(const struct m_storage *const storage,
    const size_t start_offset, const size_t end_offset)
{
  assert(start_offset <= end_offset);
  assert(end_offset <= storage->size);

  /*
   * Persist only storage contents.
   * If storage contents is synced, then the cache is guaranteed to contain
   * only valid items after program crash or restart.
   *
   * There is no need in explicit syncing of map contents, because it should be
   * automatically synced by the OS with memory page granularity.
   *
   * Map slot in index file may become corrupted if it is split by memory
   * page boundary and the OS syncs only one half of the slot before program
   * crash or exit. But it is OK, since the map automatically recovers from such
   * corruptions by clearing corrupted slots.
   */

  const size_t sync_chunk_size = end_offset - start_offset;
  if (sync_chunk_size > 0) {
    void *const ptr = m_storage_get_ptr(storage, start_offset);
    p_memory_sync(ptr, sync_chunk_size);
  }
}

static void m_sync_flush_data(struct p_lock *const cache_lock,
    const struct m_storage *const storage,
    struct ybc_item *const acquired_items_head,
    struct m_storage_cursor *const sync_cursor,
    const int has_overwrite_protection)
{
  p_lock_lock(cache_lock);
  struct m_storage_cursor next_cursor = storage->next_cursor;
  if (has_overwrite_protection) {
    m_sync_adjust_next_cursor(acquired_items_head, sync_cursor, &next_cursor);
  }
  p_lock_unlock(cache_lock);

  if (next_cursor.wrap_count == sync_cursor->wrap_count) {
    /*
     * Storage didn't wrap since the previous sync.
     * Let's sync data till next_cursor.
     */

    assert(sync_cursor->offset <= next_cursor.offset);

    m_sync_commit(storage, sync_cursor->offset, next_cursor.offset);
  }
  else {
    /*
     * Storage wrapped at least once since the previous sync.
     * Figure out which parts of storage need to be synced.
     */

    if (next_cursor.wrap_count - 1 == sync_cursor->wrap_count &&
        next_cursor.offset < sync_cursor->offset) {
      /*
       * Storage wrapped once since the previous sync.
       * Let's sync data in two steps:
       * - from sync_cursor till the end of the storage.
       * - from the beginning of the storage till next_cursor.
       */

      assert(sync_cursor->offset <= storage->size);

      m_sync_commit(storage, sync_cursor->offset, storage->size);
      m_sync_commit(storage, 0, next_cursor.offset);
    }
    else {
      /*
       * Storage wrapped more than once since the previous sync, i.e. it is full
       * of unsynced data. Let's sync the whole storage.
       */

      m_sync_commit(storage, 0, storage->size);
    }
  }

  *sync_cursor = next_cursor;
}

static void m_sync_thread_func(void *const ctx)
{
  struct m_sync *const sc = ctx;

  while (!p_event_wait_with_timeout(&sc->stop_event, sc->sync_interval)) {
    m_sync_flush_data(sc->cache_lock, sc->storage, sc->acquired_items_head,
        sc->sync_cursor, sc->has_overwrite_protection);
  }

  m_sync_flush_data(sc->cache_lock, sc->storage, sc->acquired_items_head,
      sc->sync_cursor, sc->has_overwrite_protection);
}

static void m_sync_init(struct m_sync *const sc,
    const uint64_t sync_interval, struct m_storage_cursor *const sync_cursor,
    struct m_storage *const storage,
    struct ybc_item *const acquired_items_head,
    struct p_lock *const cache_lock,
    const int has_overwrite_protection)
{
  sc->sync_interval = sync_interval;
  sc->has_overwrite_protection = has_overwrite_protection;
  sc->sync_cursor = sync_cursor;
  sc->storage = storage;
  sc->acquired_items_head = acquired_items_head;
  sc->cache_lock = cache_lock;

  if (sync_interval > 0) {
    p_event_init(&sc->stop_event);
    p_thread_init_and_start(&sc->sync_thread, &m_sync_thread_func, sc);
  }
}

static void m_sync_destroy(struct m_sync *const sc)
{
  if (sc->sync_interval > 0) {
    p_event_set(&sc->stop_event);
    p_thread_join_and_destroy(&sc->sync_thread);
    p_event_destroy(&sc->stop_event);
  }
  else {
    /* Syncing is disabled, so just update sync cursor. */
    *sc->sync_cursor = sc->storage->next_cursor;
  }
}


/*******************************************************************************
 * Dogpile effect API.
 ******************************************************************************/

/*
 * Dogpile effect item, which is pending to be added or updated in the cache.
 */
struct m_de_item
{
  /*
   * Pointer to the next item in the list.
   *
   * List's header is located in the m_de->pending_items hashtable.
   */
  struct m_de_item *next;

  /*
   * Key digest for pending item.
   */
  struct m_key_digest key_digest;

  /*
   * Expiration time for the item.
   *
   * The item is automatically removed from the list after it is expired.
   */
  uint64_t expiration_time;
};

struct m_de
{
  /*
   * The number of buckets in pending_items hash table.
   */
  size_t buckets_count;

  /*
   * Locks for pending_items buckets.
   */
  struct p_lock *locks;

  /*
   * A hash table of buckets with items, which are pending to be added
   * or updated in the cache.
   */
  struct m_de_item **pending_items;
};

static void m_de_init(struct m_de *const de, const size_t buckets_count)
{
  assert(buckets_count > 0);
  de->buckets_count = buckets_count;

  const size_t item_with_lock_size = sizeof(*de->locks) +
      sizeof(*de->pending_items);
  assert(buckets_count <= SIZE_MAX / item_with_lock_size);
  de->locks = p_malloc(buckets_count * item_with_lock_size);
  de->pending_items = (struct m_de_item **)(de->locks + buckets_count);

  for (size_t i = 0; i < buckets_count; ++i) {
    p_lock_init(&de->locks[i]);
    de->pending_items[i] = NULL;
  }
}

static void m_de_item_destroy_all(struct m_de_item *const de_item)
{
  struct m_de_item *tmp = de_item;

  while (tmp != NULL) {
    struct m_de_item *const next = tmp->next;
    p_free(tmp);
    tmp = next;
  }
}

static void m_de_destroy(struct m_de *const de)
{
  /*
   * de->pending_items can contain not-yet-expired items at destruction time.
   * It is safe removing them now.
   */
  for (size_t i = 0; i < de->buckets_count; ++i) {
    m_de_item_destroy_all(de->pending_items[i]);
    p_lock_destroy(&de->locks[i]);
  }

  p_free(de->locks);
}

static struct m_de_item *m_de_item_get(
    struct m_de_item **const pending_items_ptr,
    const struct m_key_digest *const key_digest, const uint64_t current_time)
{
  struct m_de_item **prev_ptr = pending_items_ptr;

  for (;;) {
    struct m_de_item *const de_item = *prev_ptr;

    if (de_item == NULL) {
      break;
    }

    if (de_item->expiration_time <= current_time) {
      /* Remove expired items. */
      *prev_ptr = de_item->next;
      p_free(de_item);
      continue;
    }

    if (m_key_digest_equal(&de_item->key_digest, key_digest)) {
      return de_item;
    }

    prev_ptr = &de_item->next;
  }

  return NULL;
}

static void m_de_item_set(struct m_de_item **const pending_items_ptr,
    const struct m_key_digest *const key_digest, const uint64_t expiration_time)
{
  struct m_de_item *const de_item = p_malloc(sizeof(*de_item));

  de_item->next = *pending_items_ptr;
  de_item->key_digest = *key_digest;
  de_item->expiration_time = expiration_time;

  *pending_items_ptr = de_item;
}

static int m_de_item_register(struct m_de *const de,
    const struct m_key_digest *const key_digest, const uint64_t grace_ttl)
{
  assert(grace_ttl >= C_DE_ITEM_MIN_GRACE_TTL);
  assert(grace_ttl <= C_DE_ITEM_MAX_GRACE_TTL);

  const uint64_t current_time = p_get_current_time();

  const size_t idx = m_key_digest_mod(key_digest, de->buckets_count);
  struct m_de_item **const pending_items_ptr = &de->pending_items[idx];

  p_lock_lock(&de->locks[idx]);

  struct m_de_item *const de_item = m_de_item_get(pending_items_ptr, key_digest,
      current_time);

  if (de_item == NULL) {
    /* This assertion may break in very far future. */
    assert(grace_ttl <= UINT64_MAX - current_time);

    m_de_item_set(pending_items_ptr, key_digest, grace_ttl + current_time);
  }

  p_lock_unlock(&de->locks[idx]);

  return (de_item == NULL);
}


/*******************************************************************************
 * Config API.
 ******************************************************************************/

struct ybc_config
{
  char *index_file;
  char *data_file;
  size_t map_slots_count;
  size_t data_file_size;
  size_t map_cache_slots_count;
  size_t hot_data_size;
  size_t de_hashtable_size;
  uint64_t sync_interval;
  int has_overwrite_protection;
};

size_t ybc_config_get_size(void)
{
  return sizeof(struct ybc_config);
}

void ybc_config_init(struct ybc_config *const config)
{
  config->index_file = NULL;
  config->data_file = NULL;
  config->map_slots_count = C_CONFIG_DEFAULT_MAP_SLOTS_COUNT;
  config->data_file_size = C_CONFIG_DEFAULT_DATA_SIZE;
  config->map_cache_slots_count = C_CONFIG_DEFAULT_MAP_CACHE_SLOTS_COUNT;
  config->hot_data_size = C_CONFIG_DEFAULT_HOT_DATA_SIZE;
  config->de_hashtable_size = C_CONFIG_DEFAULT_DE_HASHTABLE_SIZE;
  config->sync_interval = C_CONFIG_DEFAULT_SYNC_INTERVAL;
  config->has_overwrite_protection = 1;
}

void ybc_config_destroy(struct ybc_config *const config)
{
  p_free(config->index_file);
  p_free(config->data_file);
}

void ybc_config_set_max_items_count(struct ybc_config *const config,
    const size_t max_items_count)
{
  config->map_slots_count = max_items_count / C_MAP_OPTIMAL_FILL_RATIO;
}

void ybc_config_set_data_file_size(struct ybc_config *const config,
    const size_t size)
{
  config->data_file_size = size;
}

void ybc_config_set_index_file(struct ybc_config *const config,
    const char *const filename)
{
  p_strdup(&config->index_file, filename);
}

void ybc_config_set_data_file(struct ybc_config *const config,
    const char *const filename)
{
  p_strdup(&config->data_file, filename);
}

void ybc_config_set_hot_items_count(struct ybc_config *const config,
    const size_t hot_items_count)
{
  config->map_cache_slots_count = hot_items_count / C_MAP_OPTIMAL_FILL_RATIO;
}

void ybc_config_set_hot_data_size(struct ybc_config *const config,
    const size_t hot_data_size)
{
  config->hot_data_size = hot_data_size;
}

void ybc_config_set_de_hashtable_size(struct ybc_config *const config,
    const size_t de_hashtable_size)
{
  config->de_hashtable_size = ((de_hashtable_size > 0) ? de_hashtable_size :
      C_CONFIG_DEFAULT_DE_HASHTABLE_SIZE);
  if (config->de_hashtable_size > C_CONFIG_MAX_DE_HASHTABLE_SIZE) {
    config->de_hashtable_size = C_CONFIG_MAX_DE_HASHTABLE_SIZE;
  }
}

void ybc_config_set_sync_interval(struct ybc_config *const config,
    const uint64_t sync_interval)
{
  config->sync_interval = sync_interval;
}

void ybc_config_disable_overwrite_protection(struct ybc_config *const config)
{
  config->has_overwrite_protection = 0;
}


/*******************************************************************************
 * Cache management API
 ******************************************************************************/

struct ybc
{
  struct p_lock lock;
  struct m_index index;
  struct m_storage storage;
  struct m_sync sc;
  struct m_de de;
  struct ybc_item acquired_items_head;
  struct ybc_item acquired_items_tail;
  size_t hot_data_size;
  int has_overwrite_protection;
};

static int m_open(struct ybc *const cache,
    const struct ybc_config *const config, const int force)
{
  struct m_storage_cursor *next_cursor;
  int is_index_file_created, is_storage_file_created;

  p_memory_init();

  cache->has_overwrite_protection = config->has_overwrite_protection;
  cache->storage.size = config->data_file_size;
  m_storage_fix_size(&cache->storage.size);

  size_t map_slots_count = config->map_slots_count;
  m_map_fix_slots_count(&map_slots_count, cache->storage.size);

  size_t map_cache_slots_count = config->map_cache_slots_count;
  m_map_cache_fix_slots_count(&map_cache_slots_count, map_slots_count);

  if (!m_index_open(&cache->index, map_slots_count, map_cache_slots_count,
      config->index_file, force, &is_index_file_created, &next_cursor)) {
    return 0;
  }
  if (next_cursor->offset > cache->storage.size) {
    next_cursor->offset = 0;
  }

  cache->storage.next_cursor = *next_cursor;
  cache->storage.hash_seed = *cache->index.hash_seed_ptr;

  if (!m_storage_open(&cache->storage, config->data_file, force,
      &is_storage_file_created)) {
    m_index_close(&cache->index);
    if (is_index_file_created) {
      m_file_remove_if_exists(config->index_file);
    }
    return 0;
  }

  m_item_skiplist_init(&cache->acquired_items_head,
      &cache->acquired_items_tail, cache->storage.size);

  /*
   * Do not move initialization of the lock above, because it must be destroyed
   * in the error paths above, i.e. more lines of code is required.
   */
  p_lock_init(&cache->lock);

  m_sync_init(&cache->sc, config->sync_interval, next_cursor, &cache->storage,
      &cache->acquired_items_head, &cache->lock,
      cache->has_overwrite_protection);
  m_de_init(&cache->de, config->de_hashtable_size);

  cache->hot_data_size = config->hot_data_size;
  m_ws_fix_hot_data_size(&cache->hot_data_size, cache->storage.size);

  return 1;
}

size_t ybc_get_size(void)
{
  return sizeof(struct ybc);
}

int ybc_open(struct ybc *const cache, const struct ybc_config *const config,
    const int force)
{
  if (config == NULL) {
    struct ybc_config tmp_config;
    ybc_config_init(&tmp_config);
    int is_success = m_open(cache, &tmp_config, force);
    ybc_config_destroy(&tmp_config);
    return is_success;
  }

  return m_open(cache, config, force);
}

void ybc_close(struct ybc *const cache)
{
  m_item_skiplist_destroy(&cache->acquired_items_head,
      &cache->acquired_items_tail, cache->storage.size);

  m_de_destroy(&cache->de);

  m_sync_destroy(&cache->sc);

  p_lock_destroy(&cache->lock);

  m_storage_close(&cache->storage);

  m_index_close(&cache->index);
}

void ybc_clear(struct ybc *const cache)
{
  /*
   * New hash seed automatically invalidates all the items stored in the cache.
   */

  ++cache->storage.hash_seed;
  *cache->index.hash_seed_ptr = cache->storage.hash_seed;
}

void ybc_remove(const struct ybc_config *const config)
{
  m_file_remove_if_exists(config->index_file);
  m_file_remove_if_exists(config->data_file);
}


/*******************************************************************************
 * 'Add' transaction API.
 ******************************************************************************/

struct ybc_set_txn
{
  struct m_key_digest key_digest;
  struct ybc_item item;
};

static void *m_item_get_value_ptr(const struct ybc_item *const item)
{
  const size_t metadata_size = m_storage_metadata_get_size(item->key_size);
  assert(item->payload.size >= metadata_size);

  char *const ptr = m_storage_get_ptr(&item->cache->storage,
      item->payload.cursor.offset);
  assert((uintptr_t)ptr <= UINTPTR_MAX - metadata_size);
  return ptr + metadata_size;
}

static size_t m_item_get_size(const struct ybc_item *const item)
{
  const size_t metadata_size = m_storage_metadata_get_size(item->key_size);
  assert(item->payload.size >= metadata_size);
  return item->payload.size - metadata_size;
}

static uint64_t m_item_get_ttl(const struct ybc_item *const item)
{
  const uint64_t current_time = p_get_current_time();
  if (item->payload.expiration_time < current_time) {
    return 0;
  }

  return item->payload.expiration_time - current_time;
}

static void m_item_register(struct ybc_item *const item,
    struct ybc_item *const acquired_items_head)
{
  m_item_skiplist_get_prevs(acquired_items_head, item->next,
      item->payload.cursor.offset);
  m_item_skiplist_add(item);
}

static void m_item_deregister(struct ybc_item *const item)
{
  m_item_skiplist_del(item);
}

static void m_item_release(struct ybc_item *const item)
{
  if (item->cache->has_overwrite_protection) {
    struct p_lock *const cache_lock = &item->cache->lock;
    p_lock_lock(cache_lock);
    m_item_deregister(item);
    p_lock_unlock(cache_lock);
  }
  item->cache = NULL;
}

/*
 * Relocates the item from src to dst.
 *
 * Before the move dst must be uninitialized.
 * After the move src is considered uninitialized.
 */
static void m_item_relocate(struct ybc_item *const dst,
    struct ybc_item *const src)
{
  m_item_skiplist_relocate(dst, src);
}

size_t ybc_set_txn_get_size(void)
{
  return sizeof(struct ybc_set_txn);
}

int ybc_set_txn_begin(struct ybc *const cache, struct ybc_set_txn *const txn,
    const struct ybc_key *const key, const size_t value_size,
    const uint64_t ttl)
{
  if (value_size > SIZE_MAX - key->size) {
    return 0;
  }

  m_key_digest_get(&txn->key_digest, cache->storage.hash_seed, key);

  txn->item.cache = cache;
  txn->item.key_size = key->size;
  txn->item.is_set_txn = 1;

  const size_t metadata_size = m_storage_metadata_get_size(key->size);
  assert(value_size <= SIZE_MAX - metadata_size);
  txn->item.payload.size = metadata_size + value_size;

  const uint64_t current_time = p_get_current_time();
  txn->item.payload.expiration_time = (ttl > UINT64_MAX - current_time) ?
      UINT64_MAX : (ttl + current_time);

  p_lock_lock(&cache->lock);
  int is_success = m_storage_allocate(&cache->storage,
      &cache->acquired_items_head, &txn->item, cache->has_overwrite_protection);
  p_lock_unlock(&cache->lock);

  if (!is_success) {
    return 0;
  }

  m_storage_metadata_save(&cache->storage, &txn->item.payload, key);

  return 1;
}

void ybc_set_txn_update_value_size(struct ybc_set_txn *const txn,
    const size_t value_size)
{
  const size_t key_size = txn->item.key_size;
  const size_t metadata_size = m_storage_metadata_get_size(key_size);
  struct m_storage_payload *const payload = &txn->item.payload;

  assert(payload->size >= metadata_size);
  assert(value_size <= payload->size - metadata_size);
  const size_t old_payload_size = payload->size;
  payload->size = metadata_size + value_size;

  struct ybc *const cache = txn->item.cache;

  m_storage_metadata_update_payload_size(&cache->storage, payload,
      old_payload_size, key_size);

  // move next_cursor backwards if possible in order to conserve unused space.
  struct m_storage_cursor *const next_cursor = &cache->storage.next_cursor;
  p_lock_lock(&cache->lock);
  if (next_cursor->offset == payload->cursor.offset + old_payload_size &&
      next_cursor->wrap_count == payload->cursor.wrap_count) {
    next_cursor->offset = payload->cursor.offset + payload->size;
  }
  p_lock_unlock(&cache->lock);
}

void ybc_set_txn_commit(struct ybc_set_txn *const txn)
{
  struct ybc *const cache = txn->item.cache;

  m_map_cache_set(&cache->index.map, &cache->index.map_cache, &txn->key_digest,
      &txn->item.payload);

  m_item_release(&txn->item);
}

void ybc_set_txn_commit_item(struct ybc_set_txn *const txn,
    struct ybc_item *const item)
{
  struct ybc *const cache = txn->item.cache;

  if (cache->has_overwrite_protection) {
    p_lock_lock(&cache->lock);
    m_item_relocate(item, &txn->item);
    p_lock_unlock(&cache->lock);
  } else {
    *item = txn->item;
  }
  item->is_set_txn = 0;

  m_map_cache_set(&cache->index.map, &cache->index.map_cache, &txn->key_digest,
      &item->payload);
}

void ybc_set_txn_rollback(struct ybc_set_txn *const txn)
{
  m_item_release(&txn->item);
}

void ybc_set_txn_get_value(const struct ybc_set_txn *const txn,
    struct ybc_set_txn_value *const value)
{
  value->ptr = m_item_get_value_ptr(&txn->item);
  value->size = m_item_get_size(&txn->item);
}


/*******************************************************************************
 * Cache API.
 ******************************************************************************/

static int m_item_acquire(struct ybc *const cache, struct ybc_item *const item,
    const struct ybc_key *const key,
    const struct m_key_digest *const key_digest)
{
  item->cache = cache;
  item->key_size = key->size;
  item->is_set_txn = 0;

  if (!m_map_cache_get(&cache->index.map, &cache->index.map_cache,
      key_digest, &item->payload)) {
    return 0;
  }


  /*
   * Race condition is possible when makin a copy of cache->storage.next_cursor
   * if it is concurrently updated by other thread in m_storage_allocate().
   * In this case copied next_cursor may have corrupted value. This is OK.
   * Two innocent things may occur if next_cursor contains invalid value:
   * - 'no item' may be returned for existing item;
   * - superflouos defragmentation may occur for the item.
   *
   * This racy copy significantly improves scalability of 'get item' operation
   * if overwrite protection is disabled ( cache->has_overwrite_protection = 0).
   */
  const struct m_storage_cursor next_cursor = cache->storage.next_cursor;

  const uint64_t current_time = p_get_current_time();
  if (!m_storage_payload_check(&cache->storage, &next_cursor, &item->payload,
      current_time)) {
    return 0;
  }
  if (cache->has_overwrite_protection) {
    p_lock_lock(&cache->lock);
    m_item_register(item, &cache->acquired_items_head);
    p_lock_unlock(&cache->lock);
  }

  if (!m_storage_metadata_check(&cache->storage, &item->payload, key)) {
    m_item_release(item);
    return 0;
  }

  if (m_ws_should_defragment(&cache->storage, &next_cursor, &item->payload,
      cache->hot_data_size)) {
    m_ws_defragment(cache, item, key);
  }

  return 1;
}

size_t ybc_item_get_size(void)
{
  return sizeof(struct ybc_item);
}

int ybc_item_set(struct ybc *const cache, const struct ybc_key *const key,
    const struct ybc_value *const value)
{
  struct ybc_set_txn txn;

  if (!ybc_set_txn_begin(cache, &txn, key, value->size, value->ttl)) {
    return 0;
  }

  void *const dst = m_item_get_value_ptr(&txn.item);
  memcpy(dst, value->ptr, value->size);
  ybc_set_txn_commit(&txn);
  return 1;
}

int ybc_item_set_item(struct ybc *const cache, struct ybc_item *const item,
    const struct ybc_key *const key, const struct ybc_value *const value)
{
  struct ybc_set_txn txn;

  if (!ybc_set_txn_begin(cache, &txn, key, value->size, value->ttl)) {
    return 0;
  }

  void *const dst = m_item_get_value_ptr(&txn.item);
  memcpy(dst, value->ptr, value->size);
  ybc_set_txn_commit_item(&txn, item);
  return 1;
}

int ybc_item_remove(struct ybc *const cache, const struct ybc_key *const key)
{
  /*
   * Item with different key may be removed if it has the same key digest.
   * But it should be OK, since this is a cache, not a permanent storage.
   */

  struct m_key_digest key_digest;

  m_key_digest_get(&key_digest, cache->storage.hash_seed, key);
  return m_map_cache_remove(&cache->index.map, &cache->index.map_cache,
      &key_digest);
}

int ybc_item_get(struct ybc *const cache, struct ybc_item *const item,
    const struct ybc_key *const key)
{
  struct m_key_digest key_digest;

  m_key_digest_get(&key_digest, cache->storage.hash_seed, key);
  return m_item_acquire(cache, item, key, &key_digest);
}

static uint64_t m_item_adjust_grace_ttl(const uint64_t grace_ttl)
{
  uint64_t adjusted_grace_ttl = grace_ttl;
  if (adjusted_grace_ttl < C_DE_ITEM_MIN_GRACE_TTL) {
    adjusted_grace_ttl = C_DE_ITEM_MIN_GRACE_TTL;
  }
  else if (adjusted_grace_ttl > C_DE_ITEM_MAX_GRACE_TTL) {
    adjusted_grace_ttl = C_DE_ITEM_MAX_GRACE_TTL;
  }
  return adjusted_grace_ttl;
}

static enum ybc_de_status m_item_acquire_de_async(struct ybc *const cache,
    struct ybc_item *const item, const struct ybc_key *const key,
    const struct m_key_digest *const key_digest, const uint64_t grace_ttl)
{
  if (!m_item_acquire(cache, item, key, key_digest)) {
    /*
     * The item is missing in the cache.
     * Try registering the item in dogpile effect container. If the item
     * is successfully registered there, then allow the caller adding new item
     * by returning YBC_DE_NOTFOUND. Otherwise suggest the caller waiting
     * for a while by returning YBC_DE_WOULDBLOCK.
     */
    if (m_de_item_register(&cache->de, key_digest, grace_ttl)) {
      return YBC_DE_NOTFOUND;
    }

    return YBC_DE_WOULDBLOCK;
  }

  if (m_item_get_ttl(item) < grace_ttl) {
    /*
     * The item is about to be expired soon.
     * Try registering the item in dogpile effect container. If the item
     * is successfully registered there, then force the caller updating
     * the item by returning YBC_DE_NOTFOUND. Otherwise return not-yet expired
     * item.
     */
    if (m_de_item_register(&cache->de, key_digest, grace_ttl)) {
      m_item_release(item);
      return YBC_DE_NOTFOUND;
    }
  }

  return YBC_DE_SUCCESS;
}

enum ybc_de_status ybc_item_get_de_async(struct ybc *const cache,
    struct ybc_item *const item, const struct ybc_key *const key,
    const uint64_t grace_ttl)
{
  struct m_key_digest key_digest;
  const uint64_t adjusted_grace_ttl = m_item_adjust_grace_ttl(grace_ttl);

  m_key_digest_get(&key_digest, cache->storage.hash_seed, key);

  return m_item_acquire_de_async(cache, item, key, &key_digest,
      adjusted_grace_ttl);
}

enum ybc_de_status ybc_item_get_de(struct ybc *const cache,
    struct ybc_item *const item, const struct ybc_key *const key,
    const uint64_t grace_ttl)
{
  struct m_key_digest key_digest;
  const uint64_t adjusted_grace_ttl = m_item_adjust_grace_ttl(grace_ttl);

  m_key_digest_get(&key_digest, cache->storage.hash_seed, key);

  for (;;) {
    enum ybc_de_status status = m_item_acquire_de_async(cache, item, key,
        &key_digest, adjusted_grace_ttl);
    if (status != YBC_DE_WOULDBLOCK) {
      return status;
    }

    /*
     * Though it looks like waiting on a condition (event) would be better
     * than periodic sleeping for a fixed amount of time, this isn't true.
     *
     * Trust me - I tried using condition here, but the resulting code
     * was uglier, more intrusive, slower and less robust than the code based
     * on periodic sleepinig :)
     */
    p_sleep(C_DE_ITEM_SLEEP_TIME);
  }
}

void ybc_item_release(struct ybc_item *const item)
{
  m_item_release(item);
}

void ybc_item_get_value(const struct ybc_item *const item,
    struct ybc_value *const value)
{
  value->ptr = m_item_get_value_ptr(item);
  value->size = m_item_get_size(item);
  value->ttl = m_item_get_ttl(item);
}


/*******************************************************************************
 * Cache cluster API.
 ******************************************************************************/

struct ybc_cluster
{
  /*
   * The number of caches in the cluster.
   */
  size_t caches_count;

  /*
   * The total number of slots in all caches.
   */
  size_t total_slots_count;

  /*
   * Hash seed, which is used for selecting a cache from the cluster
   * for the given key.
   *
   * This seed mustn't match seeds used inside caches. If it will match caches'
   * seeds, then caches may suffer from uneven distribution of items
   * inside their internal hash maps.
   */
  uint64_t hash_seed;

  /*
   * The ybc_cluster structure contains also the following two 'virtual' arrays:
   *
   * struct ybc caches[caches_count];
   * size_t slot_indexes[caches_count];
   *
   * Since caches_count is determined in runtime, it is impossible declaring
   * these arrays here in plain C.
   *
   * Use m_cluster_get_caches() and m_cluster_get_max_slot_indexes() functions
   * for quick access to the corresponding arrays.
   */
};

static struct ybc *m_cluster_get_caches(struct ybc_cluster *const cluster)
{
  return (struct ybc *)(&cluster[1]);
}

static size_t *m_cluster_get_max_slot_indexes(struct ybc_cluster *const cluster,
    const size_t caches_count)
{
  assert(caches_count > 0);

  struct ybc *const caches = m_cluster_get_caches(cluster);

  return (size_t *)(&caches[caches_count]);
}

static void m_cluster_close_caches(struct ybc *const caches,
    const size_t caches_count)
{
  for (size_t i = 0; i < caches_count; ++i) {
    ybc_close(&caches[i]);
  }
}

size_t ybc_cluster_get_size(const size_t caches_count)
{
  assert(caches_count > 0);

  /*
   * A cache entry consists of ybc structure plus the corresponding
   * max_slot_index value.
   */
  const size_t cache_size = ybc_get_size() + sizeof(size_t);
  assert(caches_count <= SIZE_MAX / cache_size);
  const size_t caches_size = cache_size * caches_count;

  assert(caches_size <= SIZE_MAX - sizeof(struct ybc_cluster));
  return sizeof(struct ybc_cluster) + caches_size;
}

int ybc_cluster_open(struct ybc_cluster *const cluster,
    const struct ybc_config *const configs, const size_t caches_count,
    const int force)
{
  assert(caches_count > 0);
  struct ybc *const caches = m_cluster_get_caches(cluster);
  size_t *const max_slot_indexes = m_cluster_get_max_slot_indexes(cluster,
      caches_count);
  size_t total_slots_count = 0;

  cluster->hash_seed = C_CLUSTER_INITIAL_HASH_SEED;

  for (size_t i = 0; i < caches_count; ++i) {
    const struct ybc_config *const config = &configs[i];
    struct ybc *const cache = &caches[i];

    if (!ybc_open(cache, config, force)) {
      m_cluster_close_caches(caches, i);
      return 0;
    }

    if (total_slots_count > SIZE_MAX - config->map_slots_count) {
      m_cluster_close_caches(caches, i + 1);
      return 0;
    }

    total_slots_count += config->map_slots_count;
    max_slot_indexes[i] = total_slots_count;

    cluster->hash_seed += cache->storage.hash_seed;
  }

  cluster->caches_count = caches_count;
  cluster->total_slots_count = total_slots_count;

  return 1;
}

void ybc_cluster_close(struct ybc_cluster *const cluster)
{
  assert(cluster->caches_count > 0);

  struct ybc *const caches = m_cluster_get_caches(cluster);

  m_cluster_close_caches(caches, cluster->caches_count);

  cluster->caches_count = 0;
}

struct ybc *ybc_cluster_get_cache(struct ybc_cluster *const cluster,
    const struct ybc_key *const key)
{
  struct ybc *const caches = m_cluster_get_caches(cluster);
  const size_t *const max_slot_indexes = m_cluster_get_max_slot_indexes(cluster,
      cluster->caches_count);

  struct m_key_digest key_digest;

  m_key_digest_get(&key_digest, cluster->hash_seed, key);

  const size_t slot_index = m_key_digest_mod(&key_digest,
      cluster->total_slots_count);

  /*
   * Prefer linear search over binary search here due to the following reasons:
   * - It is simpler.
   * - It works faster on relatively short arrays.
   *
   * Linear search should be OK, since it is unlikely cache cluster will contain
   * more than 100 distinct caches.
   */
  size_t i = 0;
  while (slot_index >= max_slot_indexes[i]) {
    ++i;
    assert(i < cluster->caches_count);
  }

  return &caches[i];
}

void ybc_cluster_clear(struct ybc_cluster *cluster)
{
	assert(cluster->caches_count > 0);
	const size_t caches_count = cluster->caches_count;

	struct ybc *const caches = m_cluster_get_caches(cluster);

	for (size_t i = 0; i < caches_count; ++i) {
		ybc_clear(&caches[i]);
	}
}


/*******************************************************************************
 * Simple API.
 ******************************************************************************/

static uint32_t m_simple_crc_get(const void *const ptr, const size_t size)
{
  // TODO: use more appropriate functon here (for example, crc32).
  return (uint32_t)m_hash_get(0, ptr, size);
}

int ybc_simple_set(struct ybc *const cache, const struct ybc_key *const key,
    const struct ybc_value *const value)
{
  const uint32_t crc = m_simple_crc_get(value->ptr, value->size);
  const size_t crc_size = sizeof(crc);
  if (value->size > SIZE_MAX - crc_size) {
    return 0;
  }

  struct ybc_set_txn txn;
  const size_t value_size = value->size + crc_size;
  if (!ybc_set_txn_begin(cache, &txn, key, value_size, value->ttl)) {
    return 0;
  }

  struct ybc_set_txn_value txn_value;
  ybc_set_txn_get_value(&txn, &txn_value);
  memcpy(txn_value.ptr, &crc, crc_size);
  memcpy(((char *)txn_value.ptr) + crc_size, value->ptr, value->size);

  ybc_set_txn_commit(&txn);

  return 1;
}

int ybc_simple_get(struct ybc *const cache, const struct ybc_key *const key,
    struct ybc_value *const value)
{
  struct ybc_item item;
  if (!ybc_item_get(cache, &item, key)) {
    return 0;
  }

  struct ybc_value tmp_value;
  ybc_item_get_value(&item, &tmp_value);
  value->ttl = tmp_value.ttl;

  const size_t crc_size = sizeof(uint32_t);
  assert(tmp_value.size >= crc_size);
  const size_t actual_size = tmp_value.size - crc_size;
  if (actual_size > value->size) {
    value->size = actual_size;
    ybc_item_release(&item);
    return -1;
  }

  uint32_t actual_crc;
  memcpy(&actual_crc, tmp_value.ptr, crc_size);
  memcpy((char *)value->ptr, ((const char *)tmp_value.ptr) + crc_size,
      actual_size);
  ybc_item_release(&item);
  value->size = actual_size;

  const uint32_t expected_crc = m_simple_crc_get(value->ptr, actual_size);
  return (actual_crc == expected_crc);
}
