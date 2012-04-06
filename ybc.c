#include "ybc.h"

#include <assert.h>  /* assert */
#include <stddef.h>  /* size_t */
#include <stdint.h>  /* uint*_t */
#include <stdlib.h>  /* rand, malloc, free */
#include <string.h>  /* memcpy, memcmp, strdup, memset */


/*******************************************************************************
 * Cache implementation.
 *
 * Naming conventions:
 * - public functions and structures start with ybc_
 * - public macros and constants start with YBC_
 * - private functions and structures start with m_
 * - private macros and constants start with M_
 *
 * Coding rules:
 * - All variables, which are expected to be immutable in the given code block,
 *   MUST be declared as constants! This provides the following benefits:
 *   + It prevents from accidental modification of the given variable.
 *   + It may help dumb compilers with 'constant propagation' optimizations.
 ******************************************************************************/


int ybc_is_thread_safe(void)
{
#ifdef YBC_SINGLE_THREADED
  return 0;
#else
  return 1;
#endif
}


/*******************************************************************************
 * Aux API.
 ******************************************************************************/

#ifndef NDEBUG

/*
 * m_less_* functions are used as a workaround for compiler warnings about
 * limited ranges of arguments.
 *
 * See http://stackoverflow.com/a/8658004/274937 .
 */

static int m_less_equal(const uint64_t a, const uint64_t b)
{
  return (a <= b);
}

static int m_less(const uint64_t a, const uint64_t b)
{
  return (a < b);
}

#endif


#ifdef YBC_HAVE_LINUX_ERROR_API

#include <errno.h>   /* errno */
#include <error.h>   /* error */
#include <stdlib.h>  /* EXIT_FAILURE */

#else  /* !YBC_HAVE_LINUX_ERROR_API */
#error "Unsupported error API"
#endif


#ifdef YBC_HAVE_CLOCK_GETTIME

#include <time.h>  /* clock_gettime, timespec */

static uint64_t m_get_current_time(void)
{
  struct timespec t;
  const int rv = clock_gettime(CLOCK_REALTIME, &t);
  assert(rv == 0);
  (void)rv;

  assert(m_less_equal(t.tv_sec, (UINT64_MAX - 1000) / 1000));
  assert(m_less(t.tv_nsec, (uint64_t)1000 * 1000 * 1000));
  return ((uint64_t)t.tv_sec) * 1000 + t.tv_nsec / (1000 * 1000);
}

#else  /* !YBC_HAVE_CLOCK_GETTIME */
#error "Unsupported time implementation"
#endif


#ifdef YBC_HAVE_NANOSLEEP

#include <time.h>  /* nanosleep */

/*
 * Suspends the current thread for the given number of milliseconds.
 */
static void m_sleep(const uint64_t milliseconds)
{
  struct timespec req = {
      .tv_sec = milliseconds / 1000,
      .tv_nsec = (milliseconds % 1000) * 1000 * 1000,
  };
  struct timespec rem;

  for (;;) {
    if (nanosleep(&req, &rem) != -1) {
      return;
    }

    if (errno != EINTR) {
      error(EXIT_FAILURE, errno, "nanosleep(milliseconds=%d)", (int)milliseconds);
    }

    req = rem;
  }
}

#else  /* !YBC_HAVE_NANOSLEEP */
#error "Unsupported sleep implementation"
#endif

/*
 * Allocates the given amount of memory. Always returns non-NULL.
 */
static void *m_malloc(const size_t size)
{
  void *const ptr = malloc(size);
  if (ptr == NULL) {
    error(EXIT_FAILURE, ENOMEM, "malloc(size=%zu)", size);
  }
  return ptr;
}

/*
 * Frees memory occupied by *dst and makes a duplicate from src.
 *
 * If src is NULL, then makes *dst NULL.
 *
 * *dst must be freed with free().
 */
static void m_strdup(char **const dst, const char *const src)
{
  free(*dst);

  if (src == NULL) {
    *dst = NULL;
    return;
  }

  *dst = strdup(src);
  if (*dst == NULL) {
    error(EXIT_FAILURE, ENOMEM, "strdup(s=[%s])", src);
  }
}

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
 * Mutex API.
 ******************************************************************************/

#ifdef YBC_SINGLE_THREADED

struct m_lock
{
  char dummy;
};

static void m_lock_init(struct m_lock *const lock)
{
  (void)lock;
}

static void m_lock_destroy(struct m_lock *const lock)
{
  (void)lock;
}

static void m_lock_lock(struct m_lock *const lock)
{
  (void)lock;
}

static void m_lock_unlock(struct m_lock *const lock)
{
  (void)lock;
}

#else  /* !YBC_SINGLE_THREADED */

#ifdef YBC_HAVE_PTHREAD

#include <pthread.h>    /* pthread_* */
#include <sys/types.h>  /* pthread_*_t */

struct m_lock
{
  pthread_mutex_t mutex;
};

static void m_lock_init(struct m_lock *const lock)
{
  pthread_mutexattr_t attr;
  int rv;

  rv = pthread_mutexattr_init(&attr);
  if (rv != 0) {
    error(EXIT_FAILURE, rv, "pthread_mutexattr_init()");
  }

#ifdef NDEBUG
  const int type = PTHREAD_MUTEX_DEFAULT;
#else
  const int type = PTHREAD_MUTEX_ERRORCHECK;
#endif

  rv = pthread_mutexattr_settype(&attr, type);
  assert(rv == 0);

  rv = pthread_mutex_init(&lock->mutex, &attr);
  if (rv != 0) {
    error(EXIT_FAILURE, rv, "pthread_mutex_init()");
  }

  rv = pthread_mutexattr_destroy(&attr);
  assert(rv == 0);
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

#endif  /* !YBC_SINGLE_THREADED */

/*******************************************************************************
 * File API.
 ******************************************************************************/

#ifdef YBC_HAVE_LINUX_FILE_API

#include <fcntl.h>      /* open, posix_allocate, readahead, posix_fadvise,
                         * fcntl
                         */
#include <stdio.h>      /* tmpfile, fileno, fclose */
#include <sys/stat.h>   /* open, fstat */
#include <sys/types.h>  /* open, fstat */
#include <unistd.h>     /* close, fstat, access, unlink, dup, fcntl */

struct m_file
{
  int fd;
};

static void m_file_dup(int *const dst_fd, const int src_fd)
{
  for (;;) {
    *dst_fd = dup(src_fd);
    if (*dst_fd != -1) {

      /* Close duplicated file descriptor on exec() for security reasons. */
      if (fcntl(*dst_fd, F_SETFD, FD_CLOEXEC) == -1) {
        error(EXIT_FAILURE, errno, "fcntl(fd=%d, F_SETFD, FD_CLOEXEC)",
            *dst_fd);
      }

      return;
    }

    if (errno != EINTR) {
      error(EXIT_FAILURE, errno, "dup(fd=%d)", src_fd);
    }
  }
}

/*
 * Creates an anonymous temporary file, which will be automatically deleted
 * after the file is closed.
 */
static void m_file_create_anonymous(struct m_file *const file)
{
  for (;;) {
    FILE *const fp = tmpfile();

    if (fp != NULL) {
      const int fd = fileno(fp);
      assert(fd != -1);

      m_file_dup(&file->fd, fd);
      fclose(fp);
      return;
    }

    if (errno != EINTR) {
      error(EXIT_FAILURE, errno, "tmpfile()");
    }
  }
}

static int m_file_exists(const char *const filename)
{
  if (access(filename, F_OK) == -1) {
    if (errno != ENOENT) {
      error(EXIT_FAILURE, errno, "access(file=[%s])", filename);
    }
    return 0;
  }

  return 1;
}

static void m_file_create(struct m_file *const file, const char *const filename)
{
  const int mode = S_IRUSR | S_IWUSR;
  int flags = O_CREAT | O_TRUNC | O_RDWR;

  flags |= O_EXCL;     /* Fail if the file already exists. */
  flags |= O_CLOEXEC;  /* Close file on exec for security reasons. */
  flags |= O_NOATIME;  /* Don't update access time for performance reasons. */

  for (;;) {
    file->fd = open(filename, flags, mode);
    if (file->fd != -1) {
      return;
    }

    if (errno != EINTR) {
      error(EXIT_FAILURE, errno, "open(mode=%d, flags=%d, file=[%s])",
          mode, flags, filename);
    }
  }
}

static void m_file_open(struct m_file *const file, const char *const filename)
{
  int flags = O_RDWR;

  flags |= O_CLOEXEC;  /* Close file on exec for security reasons. */
  flags |= O_NOATIME;  /* Don't update access time for performance reasons. */

  for (;;) {
    file->fd = open(filename, flags);
    if (file->fd != -1) {
      return;
    }

    if (errno != EINTR) {
      error(EXIT_FAILURE, errno, "open(flags=%d, file=[%s])", flags, filename);
    }
  }
}

static void m_file_close(const struct m_file *const file)
{
  /*
   * Do not use fsync() before closing the file, because this may be
   * extremely slow in certain filesystems.
   * Rely on OS for flushing file's data to storage device instead.
   */

  for (;;) {
    if (close(file->fd) != -1) {
      return;
    }

    if (errno != EINTR) {
      error(EXIT_FAILURE, errno, "close(fd=%d)", file->fd);
    }
  }
}

static void m_file_remove(const char *const filename)
{
  /*
   * According to manpages, unlink() cannot generate EINTR,
   * so don't handle this case.
   */
  if (unlink(filename) == -1) {
    error(EXIT_FAILURE, errno, "unlink(file=[%s])", filename);
  }
}

static void m_file_get_size(const struct m_file *const file, size_t *const size)
{
  /*
   * According to manpages, fstat() cannot generate EINTR,
   * so don't handle this case.
   */

  struct stat st;

  if (fstat(file->fd, &st) == -1) {
    error(EXIT_FAILURE, errno, "fstat(fd=%d)", file->fd);
  }

  *size = st.st_size;
}

/*
 * Resizes the file to the given size and makes sure the underlying space
 * in the file is actually allocated (i.e. avoids creating sparse files).
 */
static void m_file_resize_and_preallocate(const struct m_file *const file,
    const size_t size)
{
  const int rv = posix_fallocate(file->fd, 0, size);
  if (rv != 0) {
    error(EXIT_FAILURE, rv, "posix_fallocate(fd=%d, size=%zu)", file->fd, size);
  }
}

static void m_file_advise_random_access(const struct m_file *const file,
    const size_t size)
{
  const int rv = posix_fadvise(file->fd, 0, size, POSIX_FADV_RANDOM);
  if (rv != 0) {
    error(EXIT_FAILURE, rv, "posix_fadvise(fd=%d, size=%zu, random)",
        file->fd, size);
  }
}

static void m_file_cache_in_ram(const struct m_file *const file,
    const size_t size)
{
  /*
   * According to manpages, readahead() cannot generate EINTR,
   * so don't handle this case.
   */
  if (readahead(file->fd, 0, size) == -1) {
    error(EXIT_FAILURE, errno, "readahead(fd=%d, size=%zu)", file->fd, size);
  }
}

#else  /* !YBC_HAVE_LINUX_FILE_API */
#error "Unsupported file implementation"
#endif


static void m_file_remove_if_exists(const char *const filename)
{
  if (filename != NULL && m_file_exists(filename)) {
    m_file_remove(filename);
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
 * Sets is_file_created to 1 if new file has been created.
 */
static int m_file_open_or_create(struct m_file *const file,
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
     *   if the file isn't fragmented. See m_file_resize_and_preallocate() call,
     *   which is aimed towards reducing file fragmentation.
     */
    m_file_create_anonymous(file);
  }
  else if (!m_file_exists(filename)) {
    if (!force) {
      return 0;
    }

    m_file_create(file, filename);
    *is_file_created = 1;
  }
  else {
    m_file_open(file, filename);
  }

  m_file_get_size(file, &actual_file_size);
  if (actual_file_size != expected_file_size) {
    if (!force) {
      m_file_close(file);
      if (*is_file_created) {
        m_file_remove(filename);
        *is_file_created = 0;
      }
      return 0;
    }

    /*
     * Pre-allocate file space at the moment in order to minimize file
     * fragmentation in the future.
     */
    m_file_resize_and_preallocate(file, expected_file_size);
  }

  return 1;
}


/*******************************************************************************
 * Memory management API
 ******************************************************************************/

#ifdef YBC_HAVE_LINUX_MMAN_API

#include <sys/mman.h>   /* mmap, munmap, msync */
#include <unistd.h>     /* sysconf */

/*
 * The page mask is determined at runtime. See m_memory_init().
 */
static size_t m_memory_page_mask = 0;

/*
 * Initializes memory API.
 *
 * This function must be called first before using other m_memory_* functions.
 * This function may be called multiple times.
 */
static void m_memory_init(void)
{
  if (m_memory_page_mask == 0) {
    const long rv = sysconf(_SC_PAGESIZE);
    assert(rv > 0);

    /*
     * Make sure that the page size is a power of 2.
     */
    if ((rv & (rv - 1)) != 0) {
      error(EXIT_FAILURE, 0, "Unexpected page size=%ld", rv);
    }

    m_memory_page_mask = (size_t)(rv - 1);
  }
  else {
    assert((m_memory_page_mask & (m_memory_page_mask + 1)) == 0);
  }
}

/*
 * Maps size bytes of the given file into memory and stores memory pointer
 * to *ptr.
 */
static void m_memory_map(void **const ptr, const struct m_file *const file,
    const size_t size)
{
  /*
   * Accodring to manpages, mmap() cannot return EINTR, so don't handle it.
   */
  *ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, file->fd, 0);
  if (*ptr == MAP_FAILED) {
    error(EXIT_FAILURE, errno, "mmap(fd=%d, size=%zu)", file->fd, size);
  }

  assert((uintptr_t)size <= UINTPTR_MAX - (uintptr_t)*ptr);
}

/*
 * Unmaps size bytes pointed by ptr from memory.
 */
static void m_memory_unmap(void *const ptr, const size_t size)
{
  /*
   * According to manpages, munmap() cannot return EINTR, so don't handle it.
   */
  if (munmap(ptr, size) == -1) {
    error(EXIT_FAILURE, errno, "munmap(ptr=%p, size=%zu)", ptr, size);
  }
}

/*
 * Flushes size bytes pointed by ptr to backing storage.
 */
static void m_memory_sync(void *const ptr, const size_t size)
{
  assert(m_memory_page_mask != 0);
  assert((m_memory_page_mask & (m_memory_page_mask + 1)) == 0);

  /*
   * Adjust ptr to the nearest floor page boundary, because msync()
   * accepts only pointers to page boundaries.
   */
  const size_t delta = ((uintptr_t)ptr) & m_memory_page_mask;
  void *const adjusted_ptr = ((char *)ptr) - delta;
  const size_t adjusted_size = size + delta;

  if (msync(adjusted_ptr, adjusted_size, MS_SYNC) == -1) {
    error(EXIT_FAILURE, errno, "msync(ptr=%p, size=%zu)",
        adjusted_ptr, adjusted_size);
  }
}

#else  /* !YBC_HAVE_LINUX_MMAN_API */
#error "Unsupported memory management implementation"
#endif


/*******************************************************************************
 * Storage API.
 ******************************************************************************/

/*
 * Minimum size of a storage in bytes.
 */
static const size_t M_STORAGE_MIN_SIZE = 4096;

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
   *
   * Item is composed of the following parts:
   * - key
   * - value
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
   * All acquired items are linked into a doubly-linked list with the head
   * at ybc->acquired_items.
   *
   * This list is traversed when determining location in the storage
   * for newly added item, so new items don't overwrite acquired items.
   */

  /*
   * A pointer to the next acquired item.
   */
  struct ybc_item *next;

  /*
   * A pointer to the previous 'next' pointer.
   */
  struct ybc_item **prev_ptr;

  /*
   * Item's value location.
   */
  struct m_storage_payload payload;
};

static void m_storage_fix_size(size_t *const size)
{
  if (*size < M_STORAGE_MIN_SIZE) {
    *size = M_STORAGE_MIN_SIZE;
  }
}

static int m_storage_open(struct m_storage *const storage,
    const char *const filename, const int force, int *const is_file_created)
{
  struct m_file file;
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

  m_memory_map(&ptr, &file, storage->size);
  assert((uintptr_t)storage->size <= UINTPTR_MAX - (uintptr_t)ptr);

  m_file_close(&file);

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
  m_memory_unmap(storage->data, storage->size);
}

static void *m_storage_get_ptr(const struct m_storage *const storage,
    const size_t offset)
{
  assert(offset <= storage->size);
  assert((uintptr_t)offset <= UINTPTR_MAX - (uintptr_t)storage->data);

  return storage->data + offset;
}

/*
 * Allocates space for new item with the given size in the storage.
 *
 * On success returns non-zero and sets up item_cursor to point to the allocated
 * space in the storage.
 *
 * On failure returns zero.
 */
static int m_storage_allocate(struct m_storage *const storage,
    const struct ybc_item *const acquired_items, const size_t size,
    struct m_storage_cursor *const item_cursor)
{
  const size_t storage_size = storage->size;

  if (size > storage_size) {
    return 0;
  }

  size_t next_offset = storage->next_cursor.offset;
  assert(next_offset <= storage_size);

  /*
   * This loop has O(n^2) worst-case performance, where n is the number
   * of items in the acquired_items list. But this is unlikely case.
   * Amortized performance is O(n), when acquired items don't interfere
   * with [next_offset ... next_offset + size) region.
   * Since acquired_items list is usually small enough (it contains only
   * currently acquired items), this loop should be quite fast in general case.
   */
  const size_t initial_offset = next_offset;
  int is_storage_wrapped = 0;
  for (;;) {
    if (next_offset > storage_size - size) {
      ++storage->next_cursor.wrap_count;
      next_offset = 0;
      is_storage_wrapped = 1;
    }

   /*
    * Make sure currently acquired items don't interfere with a region
    * of size bytes starting from next_offset.
    * Move next_offset forward if required.
    */
    const struct ybc_item *item = acquired_items;
    while (item != NULL) {
      const struct m_storage_payload *const payload = &item->payload;

      /*
       * It is expected that item's properties are already verified
       * in ybc_item_acquire(), so use only assertions here.
       */
      assert(payload->size <= storage_size);
      assert(payload->cursor.offset <= storage_size - payload->size);

      if ((next_offset < payload->cursor.offset + payload->size) &&
          (payload->cursor.offset < next_offset + size)) {
        /*
         * next_offset interferes with the item. Move next_offset
         * to the end of the item and restart the search.
         */
        next_offset = payload->cursor.offset + payload->size;
        break;
      }
      item = item->next;
    }

    if (item == NULL) {
      /* Found suitable space for the value with the given size. */
      break;
    }

    if (is_storage_wrapped && next_offset >= initial_offset) {
      /* Couldn't find a hole with appropriate size. */
      return 0;
    }
  }

  storage->next_cursor.offset = next_offset + size;

  item_cursor->wrap_count = storage->next_cursor.wrap_count;
  item_cursor->offset = next_offset;

  return 1;
}

/*
 * Checks payload correctness.
 *
 * Doesn't check item's key, which is stored in the storage, due to performance
 * reasons (avoids random memory access).
 *
 * See also m_storage_payload_key_check().
 *
 * Returns non-zero on successful check, zero on failure.
 */
static int m_storage_payload_check(const struct m_storage *const storage,
    const struct m_storage_payload *const payload)
{
  size_t max_offset = storage->next_cursor.offset;

  if (payload->expiration_time < m_get_current_time()) {
    /* The item has been expired. */
    return 0;
  }

  if (payload->cursor.wrap_count != storage->next_cursor.wrap_count) {
    if (payload->cursor.wrap_count != storage->next_cursor.wrap_count - 1) {
      /* The item is oudated or it has invalid wrap_count. */
      return 0;
    }

    if (payload->cursor.offset < storage->next_cursor.offset) {
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

/*
 * Checks key correctness for an item with the given payload.
 *
 * Item's key must match the given key.
 *
 * The function is slower than m_storage_payload_check(), because it accesses
 * random location in the storage. Use this function only if you really need it.
 *
 * Returns non-zero on successful check, zero on failure.
 */
static int m_storage_payload_key_check(const struct m_storage *const storage,
    const struct m_storage_payload *const payload,
    const struct ybc_key *const key)
{
  if (payload->size < key->size) {
    /*
     * Payload's size cannot be smaller than the key's size,
     * because key contents is stored at the beginning of payload.
     */
    return 0;
  }

  const char *ptr = m_storage_get_ptr(storage, payload->cursor.offset);
  if (memcmp(ptr, key->ptr, key->size)) {
    /* Key values mismatch. */
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
 * a lot of items with sizes smaller than VM page size (4KB). For instance,
 * a VM page can contain up to 32 items each with 128 bytes. Suppose each
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

/*
 * Items with sizes larger than the given value aren't defragmented.
 *
 * There is no reason in moving large items, which span across many VM pages.
 */
static const size_t M_WS_MAX_MOVABLE_ITEM_SIZE = 64 * 1024;

/*
 * The probability of item defragmentation if it lays outside of hot data space.
 *
 * The probability must be in the range [0..99].
 * 0 disables the defragmentation, while 99 leads to aggressive defragmentation.
 *
 * Lower probability result in smaller number of m_ws_defragment() calls
 * at the cost of probably higher hot data fragmentation.
 */
static const int M_WS_DEFRAGMENT_PROBABILITY = 10;

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
 * storage, this should be OK - subsequent threads should notice old value
 * and overwrite it with new value.
 *
 * Since this operation can be quite costly, avoid performing it in hot paths.
 */
static void m_ws_defragment(struct ybc *const cache,
    const struct ybc_item *const item, const struct ybc_key *const key)
{
  struct ybc_value value;
  struct ybc_item tmp_item;

  ybc_item_get_value(item, &value);
  if (ybc_item_add(cache, &tmp_item, key, &value)) {
    ybc_item_release(&tmp_item);
  }
}

static int m_ws_should_defragment(const struct m_storage *const storage,
    const struct m_storage_payload *const payload, const size_t hot_data_size)
{
  if (hot_data_size == 0) {
    /* Defragmentation is disabled. */
    return 0;
  }

  const size_t distance =
      (storage->next_cursor.offset >= payload->cursor.offset) ?
      (storage->next_cursor.offset - payload->cursor.offset) :
      (storage->size - (payload->cursor.offset - storage->next_cursor.offset));

  if (distance < hot_data_size) {
    /* Do not defragment recently added or defragmented items. */
    return 0;
  }

  if (payload->size > M_WS_MAX_MOVABLE_ITEM_SIZE) {
    /* Do not defragment large items. */
    return 0;
  }

  /*
   * It is OK using non-thread-safe and non-reentrant rand() here,
   * since we do not need reproducible sequence of random values.
   */
  if ((rand() % 100) >= M_WS_DEFRAGMENT_PROBABILITY) {
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
 * A digest of cache key from ybc_key structure.
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


/*******************************************************************************
 * Map API.
 ******************************************************************************/

/*
 * The map is split into buckets each with size M_MAP_BUCKET_SIZE.
 * The size of bucket containing key digests:
 *   (M_MAP_BUCKET_SIZE * sizeof(struct m_key_digest))
 * must be aligned (and probably fit) to CPU cache line for high performance,
 * because all keys in a bucket may be accessed during a lookup operation.
 */
#define M_MAP_BUCKET_MASK ((((size_t)1) << 3) - 1)
static const size_t M_MAP_BUCKET_SIZE = M_MAP_BUCKET_MASK + 1;

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
   * M_MAP_BUCKET_SIZE and M_MAP_SLOTS_COUNT_LIMIT and
   * is divided by M_MAP_BUCKET_SIZE.
   */

  if (*slots_count > data_file_size) {
    /*
     * The number of slots cannot exceed the size of data file, because
     * each slot requires at least one byte in the data file for one-byte key.
     */
    *slots_count = data_file_size;
  }

  if (*slots_count < M_MAP_BUCKET_SIZE) {
    *slots_count = M_MAP_BUCKET_SIZE;
  }

  if (*slots_count > M_MAP_SLOTS_COUNT_LIMIT) {
    *slots_count = M_MAP_SLOTS_COUNT_LIMIT;
  }

  if (*slots_count % M_MAP_BUCKET_SIZE) {
    *slots_count += M_MAP_BUCKET_SIZE - (*slots_count % M_MAP_BUCKET_SIZE);
  }
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
  assert(!m_key_digest_is_empty(key_digest));
  assert(map->slots_count % M_MAP_BUCKET_SIZE == 0);
  assert(map->slots_count >= M_MAP_BUCKET_SIZE);

  *start_index = (key_digest->digest % map->slots_count) & ~M_MAP_BUCKET_MASK;

  for (size_t i = 0; i < M_MAP_BUCKET_SIZE; ++i) {
    const size_t current_index = *start_index + i;
    assert(current_index < map->slots_count);

    if (m_key_digest_equal(&map->key_digests[current_index], key_digest)) {
      *slot_index = current_index;
      return 1;
    }
  }

  return 0;
}

static void m_map_add(const struct m_map *const map,
    const struct m_storage *const storage,
    const struct m_key_digest *const key_digest,
    const struct m_storage_payload *const payload)
{
  size_t start_index, slot_index;

  if (!m_map_lookup_slot_index(map, key_digest, &start_index, &slot_index)) {
    /* Try occupying the first empty slot in the bucket. */
    size_t i;
    size_t victim_index = start_index;
    uint64_t min_expiration_time = UINT64_MAX;

    for (i = 0; i < M_MAP_BUCKET_SIZE; ++i) {
      slot_index = start_index + i;
      assert(slot_index < map->slots_count);

      const struct m_storage_payload *const current_payload =
          &map->payloads[slot_index];

      if (m_key_digest_is_empty(&map->key_digests[slot_index]) ||
          !m_storage_payload_check(storage, current_payload)) {
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
      if (current_payload->expiration_time < min_expiration_time) {
        min_expiration_time = current_payload->expiration_time;
        victim_index = slot_index;
      }
    }

    if (i == M_MAP_BUCKET_SIZE) {
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

static void m_map_remove(const struct m_map *const map,
    const struct m_key_digest *const key_digest)
{
  size_t start_index, slot_index;

  if (m_map_lookup_slot_index(map, key_digest, &start_index, &slot_index)) {
    m_key_digest_clear(&map->key_digests[slot_index]);
  }
}

static int m_map_get(
    const struct m_map *const map, const struct m_storage *const storage,
    const struct m_key_digest *const key_digest,
    const struct ybc_key *const key, struct ybc_item *const item)
{
  size_t start_index, slot_index;

  if (!m_map_lookup_slot_index(map, key_digest, &start_index, &slot_index)) {
    return 0;
  }

  struct m_storage_payload *const payload = &map->payloads[slot_index];

  /*
   * Do not merge m_storage_payload_check() with m_storage_payload_key_check(),
   * because m_storage_payload_check() operates only on map, while
   * m_storage_payload_key_check() accesses random location in the storage.
   */
  if (!m_storage_payload_check(storage, payload) ||
      !m_storage_payload_key_check(storage, payload, key)) {
    /*
     * Optimization: clear key for invalid payload,
     * so it won't be returned and checked next time.
     */
    m_key_digest_clear(&map->key_digests[slot_index]);
    return 0;
  }

  /*
   * There is non-zero probability of invalid value slipping here.
   *
   * The following conditions must be met for this to occur:
   * - An item with the given key digest should exist.
   * - The item should have valid payload and key.
   *
   * Since this is very unlikely case, let's close eyes on it.
   * If you are paranoid, then embed integrity checking inside the value.
   */

  item->payload = *payload;
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

static void m_map_cache_init(struct m_map *const map_cache)
{
  map_cache->key_digests = NULL;
  map_cache->payloads = NULL;

  if (map_cache->slots_count == 0) {
    /* Map cache is disabled. */
    return;
  }

  assert(map_cache->slots_count <= SIZE_MAX / M_MAP_ITEM_SIZE);
  const size_t map_cache_size = map_cache->slots_count * M_MAP_ITEM_SIZE;

  map_cache->key_digests = m_malloc(map_cache_size);
  map_cache->payloads = (struct m_storage_payload *)(map_cache->key_digests +
      map_cache->slots_count);

  /*
   * Though the code can gracefully handle invalid key digests and payloads,
   * let's fill them with zeroes. This has the following benefits:
   * - It will shut up valgrind regarding comparison with uninitialized memory.
   * - It will force the OS attaching physical RAM to the allocated region
   *   of memory. This eliminates possible minor pagefaults during the cache
   *   warm-up ( http://en.wikipedia.org/wiki/Page_fault#Minor ).
   */
  memset(map_cache->key_digests, 0, map_cache_size);
}

static void m_map_cache_destroy(struct m_map *const map_cache)
{
  free(map_cache->key_digests);
}

static int m_map_cache_get(const struct m_map *const map,
    const struct m_map *const map_cache, const struct m_storage *const storage,
    const struct ybc_key *const key,
    const struct m_key_digest *const key_digest, struct ybc_item *const item)
{
  if (map_cache->slots_count == 0) {
    /* The map cache is disabled. Look up the item via map. */
    return m_map_get(map, storage, key_digest, key, item);
  }

  /*
   * Fast path: look up the item via the map cache.
   */
  if (m_map_get(map_cache, storage, key_digest, key, item)) {
    return 1;
  }

  /*
   * Slow path: fall back to looking up the item via map.
   */
  if (!m_map_get(map, storage, key_digest, key, item)) {
    return 0;
  }

  /*
   * Add the found item to the map cache.
   */
  m_map_add(map_cache, storage, key_digest, &item->payload);
  return 1;
}

static void m_map_cache_add(const struct m_map *const map,
    const struct m_map *const map_cache, const struct m_storage *const storage,
    const struct m_key_digest *const key_digest,
    const struct m_storage_payload *const payload)
{
  if (map_cache->slots_count != 0) {
    m_map_remove(map_cache, key_digest);
  }
  m_map_add(map, storage, key_digest, payload);
}

static void m_map_cache_remove(const struct m_map *const map,
    const struct m_map *const map_cache,
    const struct m_key_digest *const key_digest)
{
  if (map_cache->slots_count != 0) {
    m_map_remove(map_cache, key_digest);
  }
  m_map_remove(map, key_digest);
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
   * A pointer to the next free memory in storage file.
   *
   * This pointer points to the corresponding location in index file.
   */
  struct m_storage_cursor *sync_cursor;

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

  /*
   * An immutable copy of hash seed from index file.
   *
   * It is used instead of *hash_seed_ptr due to the following reasons:
   * - avoiding a memory dereference in hot paths during key digests'
   *   calculations.
   * - protecting from accidental (or malicious) corruption of *hash_seed_ptr
   *   located directly in index file.
   */
  uint64_t hash_seed;
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

static int m_index_open(struct m_index *const index, const char *const filename,
    const int force, int *const is_file_created)
{
  struct m_file file;
  void *ptr;

  const size_t file_size = m_index_get_file_size(index->map.slots_count);

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
   * of key slots per second) on modern HDDs and SSDs. Of course, if the index
   * file's fragmentation is low. The code tries hard achieving low
   * fragmentation by pre-allocating file contents at creation time.
   * See m_file_open_or_create() for details.
   */
  m_file_cache_in_ram(&file, file_size);

  /*
   * Hint the OS about random access pattern to index file contents.
   */
  m_file_advise_random_access(&file, file_size);

  m_memory_map(&ptr, &file, file_size);
  assert((uintptr_t)file_size <= UINTPTR_MAX - (uintptr_t)ptr);

  m_file_close(&file);

  index->sync_cursor = ptr;
  index->hash_seed_ptr = (uint64_t *)(index->sync_cursor + 1);
  if (*is_file_created) {
    *index->hash_seed_ptr = m_get_current_time();
  }
  index->hash_seed = *index->hash_seed_ptr;

  index->map.key_digests = (struct m_key_digest *)(index->hash_seed_ptr + 1);
  index->map.payloads = (struct m_storage_payload *)
      (index->map.key_digests + index->map.slots_count);

  /*
   * Do not verify correctness of loaded index file now, because the cache
   * discovers and fixes errors in the index file on the fly.
   */

  m_map_cache_init(&index->map_cache);

  return 1;
}

static void m_index_close(struct m_index *const index)
{
  m_map_cache_destroy(&index->map_cache);

  const size_t file_size = m_index_get_file_size(index->map.slots_count);

  m_memory_unmap(index->sync_cursor, file_size);
}

/*******************************************************************************
 * Sync API.
 ******************************************************************************/

/*
 * Data related to storage syncing.
 */
struct m_sync
{
  /*
   * A pointer to the next unsynced byte in the storage.
   */
  struct m_storage_cursor checkpoint_cursor;

  /*
   * Interval between data syncs.
   */
  uint64_t sync_interval;

  /*
   * Time in milliseconds for the last sync.
   */
  uint64_t last_sync_time;

  /*
   * A flag indicating whether syncing is currently running.
   *
   * This flag prevents concurrent syncing from multiple threads, because
   * it has no any benefits.
   */
  int is_syncing;
};

static void m_sync_init(struct m_sync *const sc,
    const struct m_storage_cursor *const next_cursor,
    const uint64_t sync_interval)
{
  sc->checkpoint_cursor = *next_cursor;
  sc->sync_interval = sync_interval;
  sc->last_sync_time = m_get_current_time();
  sc->is_syncing = 0;
}

static void m_sync_destroy(struct m_sync *const sc)
{
  assert(!sc->is_syncing);
  (void)sc;
}

/*
 * Starts data syncing.
 *
 * This includes:
 * - setting *start_offset to the offset in the storage, from where data syncing
 *   must start.
 * - setting *sync_chunk_size to the size of data to be synced.
 * - setting checkpoint_cursor to the position next after the data to be synced.
 */
static void m_sync_start(struct m_storage_cursor *const checkpoint_cursor,
    const struct m_storage *const storage, size_t *const start_offset,
    size_t *const sync_chunk_size)
{
  if (storage->next_cursor.wrap_count == checkpoint_cursor->wrap_count) {
    /*
     * Storage didn't wrap since the previous sync.
     * Let's sync data till storage's next_cursor.
     */

    assert(storage->next_cursor.offset >= checkpoint_cursor->offset);

    *start_offset = checkpoint_cursor->offset;
    *sync_chunk_size = storage->next_cursor.offset - checkpoint_cursor->offset;
    checkpoint_cursor->offset = storage->next_cursor.offset;
  }
  else {
    /*
     * Storage wrapped at least once since the previous sync.
     * Figure out which parts of storage need to be synced.
     */

    if (storage->next_cursor.wrap_count - 1 == checkpoint_cursor->wrap_count &&
        storage->next_cursor.offset < checkpoint_cursor->offset) {
      /*
       * Storage wrapped once since the previous sync.
       * Let's sync data starting from checkpoint_cursor till the end
       * of the storage. The rest of unsynced data will be synced next time.
       */

      assert(checkpoint_cursor->offset <= storage->size);

      *start_offset = checkpoint_cursor->offset;
      *sync_chunk_size = storage->size - checkpoint_cursor->offset;
      checkpoint_cursor->offset = 0;
    }
    else {
      /*
       * Storage wrapped more than once since the previous sync, i.e. it is full
       * of unsynced data. Let's sync the whole storage.
       */

      *start_offset = 0;
      *sync_chunk_size = storage->size;
      checkpoint_cursor->offset = storage->next_cursor.offset;
    }

    checkpoint_cursor->wrap_count = storage->next_cursor.wrap_count;
  }
}

static int m_sync_begin(struct m_sync *const sc,
    const struct m_storage *const storage, const uint64_t current_time,
    size_t *const start_offset, size_t *const sync_chunk_size)
{
  assert(current_time >= sc->last_sync_time);

  if (sc->sync_interval == 0) {
    /*
     * Syncing is disabled.
     */
    return 0;
  }

  if (sc->is_syncing) {
    /*
     * Another thread is syncing the cache at the moment,
     * so don't interfere with it.
     */
    return 0;
  }

  if (current_time - sc->last_sync_time < sc->sync_interval) {
    /*
     * Throttle too frequent syncing.
     */
    return 0;
  }

  m_sync_start(&sc->checkpoint_cursor, storage, start_offset, sync_chunk_size);

  sc->last_sync_time = current_time;
  sc->is_syncing = 1;
  return 1;
}

static void m_sync_commit(struct m_sync *const sc,
    const struct m_storage *const storage, struct m_lock *const lock,
    const size_t start_offset, size_t sync_chunk_size,
    struct m_storage_cursor *const sync_cursor)
{
  /*
   * Persist only storage contents into data file.
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

  void *const ptr = m_storage_get_ptr(storage, start_offset);

  assert(sync_chunk_size <= storage->size);
  assert(start_offset <= storage->size - sync_chunk_size);

  m_memory_sync(ptr, sync_chunk_size);

  m_lock_lock(lock);

  *sync_cursor = sc->checkpoint_cursor;

  assert(sc->is_syncing);
  sc->is_syncing = 0;

  m_lock_unlock(lock);
}


/*******************************************************************************
 * Dogpile effect API.
 ******************************************************************************/

/*
 * Minimum grace ttl, which can be passed to ybc_item_acquire_de().
 *
 * There is no sense in grace ttl, which is smaller than 1 millisecond.
 */
static const uint64_t M_DE_ITEM_MIN_GRACE_TTL = 1;

/*
 * Maximum grace ttl, which can be passed to ybc_item_acquire_de().
 *
 * 10 minutes should be enough for any practical purposes ;)
 */
static const uint64_t M_DE_ITEM_MAX_GRACE_TTL = 10 * 60 * 1000;

/*
 * Time to sleep in milliseconds before the next try on obtaining
 * not-yet-existing item.
 */
static const uint64_t M_DE_ITEM_SLEEP_TIME = 100;

/*
 * Dogpile effect item, which is pending to be added or updated in the cache.
 */
struct m_de_item
{
  /*
   * Pointer to the next item in the list.
   *
   * List's header is located in the m_de->pending_items.
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
   * Lock for pending_items list.
   */
  struct m_lock lock;

  /*
   * A list of items, which are pending to be added or updated in the cache.
   */
  struct m_de_item *pending_items;
};

static void m_de_init(struct m_de *const de)
{
  m_lock_init(&de->lock);
  de->pending_items = NULL;
}

static void m_de_item_destroy_all(struct m_de_item *const de_item)
{
  struct m_de_item *tmp = de_item;

  while (tmp != NULL) {
    struct m_de_item *const next = tmp->next;
    free(tmp);
    tmp = next;
  }
}

static void m_de_destroy(struct m_de *const de)
{
  /*
   * de->pending_items can contain not-yet-expired items at destruction time.
   * It is safe removing them now.
   */
  m_de_item_destroy_all(de->pending_items);

  m_lock_destroy(&de->lock);
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
      free(de_item);
      continue;
    }

    if (m_key_digest_equal(&de_item->key_digest, key_digest)) {
      return de_item;
    }

    prev_ptr = &de_item->next;
  }

  return NULL;
}

static void m_de_item_add(struct m_de_item **const pending_items_ptr,
    const struct m_key_digest *const key_digest, const uint64_t expiration_time)
{
  struct m_de_item *const de_item = m_malloc(sizeof(*de_item));

  de_item->next = *pending_items_ptr;
  de_item->key_digest = *key_digest;
  de_item->expiration_time = expiration_time;

  *pending_items_ptr = de_item;
}

static int m_de_item_register(struct m_de *const de,
    const struct m_key_digest *const key_digest, const uint64_t grace_ttl)
{
  assert(grace_ttl >= M_DE_ITEM_MIN_GRACE_TTL);
  assert(grace_ttl <= M_DE_ITEM_MAX_GRACE_TTL);

  const uint64_t current_time = m_get_current_time();

  m_lock_lock(&de->lock);

  struct m_de_item *const de_item = m_de_item_get(&de->pending_items,
      key_digest, current_time);

  if (de_item == NULL) {
    /* This assertion will break in very far future. */
    assert(grace_ttl <= UINT64_MAX - current_time);

    m_de_item_add(&de->pending_items, key_digest, grace_ttl + current_time);
  }

  m_lock_unlock(&de->lock);

  return (de_item == NULL);
}


/*******************************************************************************
 * Config API.
 ******************************************************************************/

static const size_t M_CONFIG_DEFAULT_MAP_SLOTS_COUNT = 100 * 1000;

static const size_t M_CONFIG_DEFAULT_DATA_SIZE = 64 * 1024 * 1024;

static const size_t M_CONFIG_DEFAULT_MAP_CACHE_SLOTS_COUNT = 10 * 1000;

static const size_t M_CONFIG_DEFAULT_HOT_DATA_SIZE = 8 * 1024 * 1024;

static const size_t M_CONFIG_DEFAULT_SYNC_INTERVAL = 10 * 1000;

struct ybc_config
{
  char *index_file;
  char *data_file;
  size_t map_slots_count;
  size_t data_file_size;
  size_t map_cache_slots_count;
  size_t hot_data_size;
  uint64_t sync_interval;
};

size_t ybc_config_get_size(void)
{
  return sizeof(struct ybc_config);
}

void ybc_config_init(struct ybc_config *const config)
{
  config->index_file = NULL;
  config->data_file = NULL;
  config->map_slots_count = M_CONFIG_DEFAULT_MAP_SLOTS_COUNT;
  config->data_file_size = M_CONFIG_DEFAULT_DATA_SIZE;
  config->map_cache_slots_count = M_CONFIG_DEFAULT_MAP_CACHE_SLOTS_COUNT;
  config->hot_data_size = M_CONFIG_DEFAULT_HOT_DATA_SIZE;
  config->sync_interval = M_CONFIG_DEFAULT_SYNC_INTERVAL;
}

void ybc_config_destroy(struct ybc_config *const config)
{
  free(config->index_file);
  free(config->data_file);
}

void ybc_config_set_max_items_count(struct ybc_config *const config,
    const size_t max_items_count)
{
  config->map_slots_count = max_items_count;
}

void ybc_config_set_data_file_size(struct ybc_config *const config,
    const size_t size)
{
  config->data_file_size = size;
}

void ybc_config_set_index_file(struct ybc_config *const config,
    const char *const filename)
{
  m_strdup(&config->index_file, filename);
}

void ybc_config_set_data_file(struct ybc_config *const config,
    const char *const filename)
{
  m_strdup(&config->data_file, filename);
}

void ybc_config_set_hot_items_count(struct ybc_config *const config,
    const size_t hot_items_count)
{
  config->map_cache_slots_count = hot_items_count;
}

void ybc_config_set_hot_data_size(struct ybc_config *const config,
    const size_t hot_data_size)
{
  config->hot_data_size = hot_data_size;
}

void ybc_config_set_sync_interval(struct ybc_config *const config,
    const uint64_t sync_interval)
{
  config->sync_interval = sync_interval;
}


/*******************************************************************************
 * Cache management API
 ******************************************************************************/

struct ybc
{
  struct m_lock lock;
  struct m_index index;
  struct m_storage storage;
  struct m_sync sc;
  struct m_de de;
  struct ybc_item *acquired_items;
  size_t hot_data_size;
};

static int m_open(struct ybc *const cache,
    const struct ybc_config *const config, const int force)
{
  int is_index_file_created, is_storage_file_created;

  m_memory_init();

  cache->storage.size = config->data_file_size;
  m_storage_fix_size(&cache->storage.size);

  cache->index.map.slots_count = config->map_slots_count;
  m_map_fix_slots_count(&cache->index.map.slots_count, cache->storage.size);

  cache->index.map_cache.slots_count = config->map_cache_slots_count;
  m_map_cache_fix_slots_count(&cache->index.map_cache.slots_count,
      cache->index.map.slots_count);

  if (!m_index_open(&cache->index, config->index_file, force,
      &is_index_file_created)) {
    return 0;
  }

  cache->storage.next_cursor = *cache->index.sync_cursor;

  if (!m_storage_open(&cache->storage, config->data_file, force,
      &is_storage_file_created)) {
    m_index_close(&cache->index);
    if (is_index_file_created) {
      m_file_remove(config->index_file);
    }
    return 0;
  }

  m_sync_init(&cache->sc, &cache->storage.next_cursor, config->sync_interval);
  m_de_init(&cache->de);

  /*
   * Do not move initialization of the lock above, because it must be destroyed
   * in the error paths above, i.e. more lines of code is required.
   */
  m_lock_init(&cache->lock);

  cache->acquired_items = NULL;
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
  assert(cache->acquired_items == NULL);

  /*
   * Flush actual cursor to index file in order
   * to persist recently added items.
   */
  *cache->index.sync_cursor = cache->storage.next_cursor;

  m_lock_destroy(&cache->lock);

  m_de_destroy(&cache->de);

  m_sync_destroy(&cache->sc);

  m_storage_close(&cache->storage);

  m_index_close(&cache->index);
}

void ybc_clear(struct ybc *const cache)
{
  /*
   * New hash seed automatically invalidates all the items stored in the cache.
   */

  /*
   * Use addition operation instead of xor for calculation new hash seed,
   * because xor may result in very low entropy if the previous hash seed
   * has been generated recently.
   */
  *cache->index.hash_seed_ptr += m_get_current_time();
  cache->index.hash_seed = *cache->index.hash_seed_ptr;
}

void ybc_remove(const struct ybc_config *const config)
{
  m_file_remove_if_exists(config->index_file);
  m_file_remove_if_exists(config->data_file);
}


/*******************************************************************************
 * 'Add' transaction API.
 ******************************************************************************/

struct ybc_add_txn
{
  struct m_key_digest key_digest;
  struct ybc_item item;
};

static void *m_item_get_value_ptr(const struct ybc_item *const item)
{
  assert(item->payload.size >= item->key_size);

  char *const ptr = m_storage_get_ptr(&item->cache->storage,
      item->payload.cursor.offset);
  assert((uintptr_t)ptr <= UINTPTR_MAX - item->key_size);
  return ptr + item->key_size;
}

static uint64_t m_item_get_ttl(const struct ybc_item *const item)
{
  const uint64_t current_time = m_get_current_time();
  if (item->payload.expiration_time < current_time) {
    return 0;
  }

  return item->payload.expiration_time - current_time;
}

static void m_item_register(struct ybc_item *const item,
    struct ybc_item **const acquired_items_ptr)
{
  item->next = *acquired_items_ptr;
  item->prev_ptr = acquired_items_ptr;
  if (item->next != NULL) {
    assert(item->next->prev_ptr == acquired_items_ptr);
    item->next->prev_ptr = &item->next;
  }
  *acquired_items_ptr = item;
}

static void m_item_deregister(struct ybc_item *const item)
{
  *item->prev_ptr = item->next;
  if (item->next != NULL) {
    assert(item->next->prev_ptr == &item->next);
    item->next->prev_ptr = item->prev_ptr;
    item->next = NULL;
  }

  item->prev_ptr = NULL;
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
  *dst = *src;

  /*
   * Fix up pointers to the item.
   */
  if (dst->next != NULL) {
    dst->next->prev_ptr = &dst->next;
  }
  *dst->prev_ptr = dst;

  src->next = NULL;
  src->prev_ptr = NULL;
}

size_t ybc_add_txn_get_size(void)
{
  return sizeof(struct ybc_add_txn);
}

int ybc_add_txn_begin(struct ybc *const cache, struct ybc_add_txn *const txn,
    const struct ybc_key *const key, const size_t value_size)
{
  if (value_size > SIZE_MAX - key->size) {
    return 0;
  }

  m_key_digest_get(&txn->key_digest, cache->index.hash_seed, key);

  txn->item.cache = cache;
  txn->item.key_size = key->size;

  assert(value_size <= SIZE_MAX - key->size);
  txn->item.payload.size = key->size + value_size;

  /*
   * Allocate storage space for the new item.
   */
  m_lock_lock(&cache->lock);
  int is_success = m_storage_allocate(&cache->storage, cache->acquired_items,
      txn->item.payload.size, &txn->item.payload.cursor);
  if (is_success) {
    m_item_register(&txn->item, &cache->acquired_items);
  }
  m_lock_unlock(&cache->lock);

  if (!is_success) {
    return 0;
  }

  /*
   * Copy key to the beginning of the storage space.
   */
  char *const ptr = m_storage_get_ptr(&cache->storage,
      txn->item.payload.cursor.offset);
  memcpy(ptr, key->ptr, key->size);

  return 1;
}

void ybc_add_txn_commit(struct ybc_add_txn *const txn,
    struct ybc_item *const item, const uint64_t ttl)
{
  struct ybc *const cache = txn->item.cache;
  const uint64_t current_time = m_get_current_time();
  size_t sync_chunk_size, start_offset;
  int should_sync;

  txn->item.payload.expiration_time = (ttl > UINT64_MAX - current_time) ?
      (UINT64_MAX) : (ttl + current_time);

  m_lock_lock(&cache->lock);

  m_map_cache_add(&cache->index.map, &cache->index.map_cache, &cache->storage,
      &txn->key_digest, &txn->item.payload);

  m_item_relocate(item, &txn->item);

  should_sync = m_sync_begin(&cache->sc, &cache->storage, current_time,
      &start_offset, &sync_chunk_size);

  m_lock_unlock(&cache->lock);

  if (should_sync) {
    m_sync_commit(&cache->sc, &cache->storage, &cache->lock,
        start_offset, sync_chunk_size, cache->index.sync_cursor);
  }
}

void ybc_add_txn_rollback(struct ybc_add_txn *const txn)
{
  m_item_deregister(&txn->item);
}

void *ybc_add_txn_get_value_ptr(const struct ybc_add_txn *const txn)
{
  return m_item_get_value_ptr(&txn->item);
}


/*******************************************************************************
 * Cache API.
 ******************************************************************************/

static int m_item_acquire(struct ybc *const cache, struct ybc_item *const item,
    const struct ybc_key *const key,
    const struct m_key_digest *const key_digest)
{
  int should_defragment = 0;
  int is_found;

  item->cache = cache;
  item->key_size = key->size;

  m_lock_lock(&cache->lock);
  is_found = m_map_cache_get(&cache->index.map, &cache->index.map_cache,
      &cache->storage, key, key_digest, item);
  if (is_found) {
    m_item_register(item, &cache->acquired_items);
    should_defragment = m_ws_should_defragment(&cache->storage, &item->payload,
        cache->hot_data_size);
  }
  m_lock_unlock(&cache->lock);

  if (should_defragment) {
    m_ws_defragment(cache, item, key);
  }

  return is_found;
}

static void m_item_release(struct ybc_item *const item)
{
  struct ybc *const cache = item->cache;

  m_lock_lock(&cache->lock);
  m_item_deregister(item);
  m_lock_unlock(&cache->lock);

  item->cache = NULL;
}

size_t ybc_item_get_size(void)
{
  return sizeof(struct ybc_item);
}

int ybc_item_add(struct ybc *const cache, struct ybc_item *const item,
    const struct ybc_key *const key, const struct ybc_value *const value)
{
  struct ybc_add_txn txn;

  if (!ybc_add_txn_begin(cache, &txn, key, value->size)) {
    return 0;
  }

  void *const dst = ybc_add_txn_get_value_ptr(&txn);
  memcpy(dst, value->ptr, value->size);
  ybc_add_txn_commit(&txn, item, value->ttl);
  return 1;
}

void ybc_item_remove(struct ybc *const cache, const struct ybc_key *const key)
{
  /*
   * Item with different key may be removed if it has the same key digest.
   * But it should be OK, since this is a cache, not permanent storage.
   */

  struct m_key_digest key_digest;

  m_key_digest_get(&key_digest, cache->index.hash_seed, key);

  m_lock_lock(&cache->lock);
  m_map_cache_remove(&cache->index.map, &cache->index.map_cache, &key_digest);
  m_lock_unlock(&cache->lock);
}

int ybc_item_acquire(struct ybc *const cache, struct ybc_item *const item,
    const struct ybc_key *const key)
{
  struct m_key_digest key_digest;

  m_key_digest_get(&key_digest, cache->index.hash_seed, key);

  return m_item_acquire(cache, item, key, &key_digest);
}

int ybc_item_acquire_de(struct ybc *const cache, struct ybc_item *const item,
    const struct ybc_key *const key, const uint64_t grace_ttl)
{
  struct m_key_digest key_digest;

  uint64_t adjusted_grace_ttl = grace_ttl;
  if (adjusted_grace_ttl < M_DE_ITEM_MIN_GRACE_TTL) {
    adjusted_grace_ttl = M_DE_ITEM_MIN_GRACE_TTL;
  }
  else if (adjusted_grace_ttl > M_DE_ITEM_MAX_GRACE_TTL) {
    adjusted_grace_ttl = M_DE_ITEM_MAX_GRACE_TTL;
  }

  m_key_digest_get(&key_digest, cache->index.hash_seed, key);

  while (!m_item_acquire(cache, item, key, &key_digest)) {
    /*
     * The item is missing in the cache.
     * Try registering the item in dogpile effect container. If the item
     * is successfully registered there, then allow the caller adding new item
     * to the cache. Otherwise wait until either the item is added by another
     * thread or its' grace ttl expires.
     */
    if (!m_de_item_register(&cache->de, &key_digest, adjusted_grace_ttl)) {
      /*
       * Though it looks like waiting on a condition (event) would be better
       * than periodic sleeping for a fixed amount of time, this isn't true.
       *
       * I tried using condition here, but the resulting code was uglier,
       * more intrusive, slower and less robust than the code based
       * on periodic sleeping.
       */
      m_sleep(M_DE_ITEM_SLEEP_TIME);
      continue;
    }

    return 0;
  }

  if (m_item_get_ttl(item) < adjusted_grace_ttl) {
    /*
     * The item is about to be expired soon.
     * Try registering the item in dogpile effect container. If the item
     * is successfully registered there, then force the caller updating
     * the item by returning an empty item. Otherwise return not-yet expired
     * item.
     */
    if (m_de_item_register(&cache->de, &key_digest, adjusted_grace_ttl)) {
      m_item_release(item);
      return 0;
    }
  }

  return 1;
}

void ybc_item_release(struct ybc_item *const item)
{
  m_item_release(item);
}

void ybc_item_get_value(const struct ybc_item *const item,
    struct ybc_value *const value)
{
  value->ptr = m_item_get_value_ptr(item);

  assert(item->payload.size >= item->key_size);
  value->size = item->payload.size - item->key_size;

  value->ttl = m_item_get_ttl(item);
}


/*******************************************************************************
 * Cache cluster API.
 ******************************************************************************/

static const uint64_t M_CLUSTER_INITIAL_HASH_SEED = 0xDEADBEEFDEADBEEF;

struct ybc_cluster
{
  size_t caches_count;
  size_t total_slots_count;
  uint64_t hash_seed;
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

  cluster->hash_seed = M_CLUSTER_INITIAL_HASH_SEED;

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

    cluster->hash_seed += cache->index.hash_seed;
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

  const size_t slot_index = key_digest.digest % cluster->total_slots_count;
  size_t i = 0;
  while (slot_index >= max_slot_indexes[i]) {
    ++i;
    assert(i < cluster->caches_count);
  }

  return &caches[i];
}
