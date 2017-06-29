/*******************************************************************************
 * Platform-specific functions' implementation for Linux.
 *
 * Naming conventions:
 * - platform-specific functions and structures must start with p_
 * - private functions and structures must start with m_
 * - private macros and constants must start with M_
 *
 * Coding rules:
 * - all (including platform-specific) function must be declared as static.
 * - All variables, which are expected to be immutable in the given code block,
 *   MUST be declared as constants! This provides the following benefits:
 *   + It prevents from accidental modification of the given variable.
 *   + It may help dumb compilers with 'constant propagation' optimizations.
 ******************************************************************************/

#define _GNU_SOURCE

#include <assert.h>     /* assert */
#include <errno.h>      /* errno */
#include <error.h>      /* error */
#include <fcntl.h>      /* open, posix_fadvise, fcntl */
#include <pthread.h>    /* pthread_* */
#include <stddef.h>     /* size_t */
#include <stdint.h>     /* uint*_t */
#include <stdio.h>      /* tmpfile, fileno, fclose */
#include <stdlib.h>     /* malloc, free, EXIT_FAILURE */
#include <string.h>     /* strdup */
#include <sys/mman.h>   /* mmap, munmap, msync */
#include <sys/stat.h>   /* open, fstat */
#include <sys/types.h>  /* pthread_*_t, open, stat, lseek */
#include <time.h>       /* clock_gettime, timespec, nanosleep */
#include <unistd.h>     /* close, fstat, access, unlink, dup, fcntl, sysconf, read,
                         * lseek, read, write
                         */

#ifndef O_CLOEXEC
  #define O_CLOEXEC 0
#endif


static void *p_malloc(const size_t size)
{
  void *const ptr = malloc(size);
  if (ptr == NULL) {
    error(EXIT_FAILURE, ENOMEM, "malloc(size=%zu)", size);
  }
  return ptr;
}

static void p_free(void *const ptr)
{
  free(ptr);
}

static void p_strdup(char **const dst, const char *const src)
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

static uint64_t p_get_current_time(void)
{
  struct timespec t;

  /*
   * Though CLOCK_MONOTONIC cannot jump backwards unlike CLOCK_REALTIME,
   * it may be inconsistent between system reboots or between distinct systems.
   * Such inconsistency makes cache persistence a joke, since all expiration
   * times in persistent cache may become arbitrarily skewed after system reboot
   * or cache files' migration to another system.
   */
  const int rv = clock_gettime(CLOCK_REALTIME, &t);
  assert(rv == 0);
  (void)rv;

  assert(m_less_equal(t.tv_sec, (UINT64_MAX - 1000) / 1000));
  assert(m_less(t.tv_nsec, (uint64_t)1000 * 1000 * 1000));
  return ((uint64_t)t.tv_sec) * 1000 + t.tv_nsec / (1000 * 1000);
}

static void p_sleep(const uint64_t milliseconds)
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

struct p_thread
{
  pthread_t t;
  p_thread_func func;
  void *ctx;
};

static void *m_thread_main_func(void *const ctx)
{
  struct p_thread *const t = ctx;

  t->func(t->ctx);

  return NULL;
}

static void p_thread_init_and_start(struct p_thread *const t,
    const p_thread_func func, void *const ctx)
{
  t->func = func;
  t->ctx = ctx;

  const int rv = pthread_create(&t->t, NULL, m_thread_main_func, t);
  if (rv != 0) {
    error(EXIT_FAILURE, rv, "pthread_create()");
  }
}

static void p_thread_join_and_destroy(struct p_thread *const t)
{
  const int rv = pthread_join(t->t, NULL);
  assert(rv == 0);
  (void)rv;
}

struct p_lock
{
  pthread_mutex_t mutex;
};

static void p_lock_init(struct p_lock *const lock)
{
  pthread_mutexattr_t attr;
  int rv;

  rv = pthread_mutexattr_init(&attr);
  if (rv != 0) {
    error(EXIT_FAILURE, rv, "pthread_mutexattr_init()");
  }

#ifdef NDEBUG
  const int type = PTHREAD_MUTEX_NORMAL;
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

static void p_lock_destroy(struct p_lock *const lock)
{
  const int rv = pthread_mutex_destroy(&lock->mutex);
  assert(rv == 0);
  (void)rv;
}

static void p_lock_lock(struct p_lock *const lock)
{
  const int rv = pthread_mutex_lock(&lock->mutex);
  assert(rv == 0);
  (void)rv;
}

static void p_lock_unlock(struct p_lock *const lock)
{
  const int rv = pthread_mutex_unlock(&lock->mutex);
  assert(rv == 0);
  (void)rv;
}

struct p_event
{
  pthread_cond_t cond;
  pthread_mutex_t mutex;
  int is_set;
};

static void p_event_init(struct p_event *const e)
{
  int rv = pthread_cond_init(&e->cond, NULL);
  if (rv != 0) {
    error(EXIT_FAILURE, rv, "pthread_cond_init()");
  }

  rv = pthread_mutex_init(&e->mutex, NULL);
  if (rv != 0) {
    error(EXIT_FAILURE, rv, "pthread_mutex_init()");
  }

  e->is_set = 0;
}

static void p_event_destroy(struct p_event *const e)
{
  int rv;

  rv = pthread_mutex_destroy(&e->mutex);
  assert(rv == 0);

  rv = pthread_cond_destroy(&e->cond);
  assert(rv == 0);

  (void)rv;
}

static void p_event_set(struct p_event *const e)
{
  int rv;

  rv = pthread_mutex_lock(&e->mutex);
  assert(rv == 0);

  e->is_set = 1;
  rv = pthread_cond_broadcast(&e->cond);
  assert(rv == 0);

  rv = pthread_mutex_unlock(&e->mutex);
  assert(rv == 0);

  (void)rv;
}

static int p_event_wait_with_timeout(struct p_event *const e,
    const uint64_t timeout)
{
  int rv;
  int is_set;

  rv = pthread_mutex_lock(&e->mutex);
  assert(rv == 0);

  if (!e->is_set) {
    const uint64_t current_time = p_get_current_time();
    const uint64_t exp_time = ((timeout <= UINT64_MAX - current_time) ?
        (current_time + timeout) : UINT64_MAX);

    struct timespec t = {
        .tv_sec = exp_time / 1000,
        .tv_nsec = (exp_time % 1000) * 1000 * 1000,
    };
    rv = pthread_cond_timedwait(&e->cond, &e->mutex, &t);
    if (rv != ETIMEDOUT) {
      assert(rv == 0);
    }
  }
  is_set = e->is_set;

  rv = pthread_mutex_unlock(&e->mutex);
  assert(rv == 0);

  return is_set;
}

struct p_file
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

static void p_file_create_anonymous(struct p_file *const file)
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

static int p_file_exists(const char *const filename)
{
  if (access(filename, F_OK) == -1) {
    if (errno != ENOENT) {
      error(EXIT_FAILURE, errno, "access(file=[%s])", filename);
    }
    return 0;
  }

  return 1;
}

static void p_file_create(struct p_file *const file, const char *const filename)
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

static void p_file_open(struct p_file *const file, const char *const filename)
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

static void p_file_close(const struct p_file *const file)
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

static void p_file_remove(const char *const filename)
{
  /*
   * According to manpages, unlink() cannot generate EINTR,
   * so don't handle this case.
   */
  if (unlink(filename) == -1) {
    error(EXIT_FAILURE, errno, "unlink(file=[%s])", filename);
  }
}

static void p_file_get_size(const struct p_file *const file, size_t *const size)
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

static void m_file_seek_zero(const struct p_file *const file) {
  const off_t off = lseek(file->fd, 0, SEEK_SET);
  if (off == -1) {
    error(EXIT_FAILURE, off, "lseek(fd=%d, 0)", file->fd);
  }
}

static void p_file_resize_and_preallocate(const struct p_file *const file,
    const size_t size)
{
  /*
   * Do not use posix_fallocate(), since it cheats and doesn't really
   * allocate pyhsical space on the storage.
   *
   * Just fill the file with garbage.
   */

  m_file_seek_zero(file);

  const size_t buf_size = 1024 * 1024;
  char *const buf = p_malloc(buf_size);

  size_t remain = size;
  while (remain) {
    size_t n = buf_size;
    if (remain < buf_size) {
      n = remain;
    }
    const int rv = write(file->fd, buf, n);
    if (rv == -1 && errno != EINTR) {
      error(EXIT_FAILURE, rv, "write(fd=%d, size=%zu)", file->fd, n);
    }
    assert((size_t)rv <= n);
    remain -= rv;
  }

  p_free(buf);

  m_file_seek_zero(file);
}

static void p_file_advise_random_access(const struct p_file *const file,
    const size_t size)
{
  const int rv = posix_fadvise(file->fd, 0, size, POSIX_FADV_RANDOM);
  if (rv != 0) {
    error(EXIT_FAILURE, rv, "posix_fadvise(fd=%d, size=%zu, random)",
        file->fd, size);
  }
}

static void p_file_cache_in_ram(const struct p_file *const file)
{
  /*
   * Do not use readahead(), since it returns immediately instead of waiting
   * until the file is read into RAM.
   */

  m_file_seek_zero(file);

  const size_t buf_size = 1024 * 1024;
  char *const buf = p_malloc(buf_size);

  for (;;) {
    const ssize_t rv = read(file->fd, buf, buf_size);
    if (rv == -1 && errno != EINTR) {
      error(EXIT_FAILURE, rv, "read(fd=%d, size=%zu)", file->fd, sizeof(buf));
    }
    if (rv == 0) {
      /* end of file */
      break;
    }
  }

  p_free(buf);

  m_file_seek_zero(file);
}

/*
 * The page mask is determined at runtime. See p_memory_init().
 */
static size_t m_memory_page_mask = 0;

static size_t p_memory_page_mask(void) {
  return m_memory_page_mask;
}

static void p_memory_init(void)
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

static void p_memory_map(void **const ptr, const struct p_file *const file,
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

static void p_memory_unmap(void *const ptr, const size_t size)
{
  /*
   * According to manpages, munmap() cannot return EINTR, so don't handle it.
   */
  if (munmap(ptr, size) == -1) {
    error(EXIT_FAILURE, errno, "munmap(ptr=%p, size=%zu)", ptr, size);
  }
}

static void p_memory_sync(void *const ptr, const size_t size)
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
