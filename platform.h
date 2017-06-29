#ifndef YBC_PLATFORM_H_INCLUDED
#define YBC_PLATFORM_H_INCLUDED

/*******************************************************************************
 * Platform-specific functions for ybc.c.
 *
 * Files containing implementations of platform-specific functions for various
 * platforms, must be included directly from this file, so they are compiled
 * as a part of ybc.c compilation unit. This has the following benefits
 * comparing to a separate compilation unit for these functions:
 * - allows direct embedding of platform-specific structures into ybc
 * structures, thus avoiding dynamic memory allocations, improving
 * memory locality and opening additional optimization opportunities
 * for compilers.
 * - allows compilers inlining platform-specific functions and performing deeper
 * inter-procedure analysis.
 *
 * Conventions:
 * - platform-specific functions and structures must start with p_
 * - platform-specific functions must be declared as static (i.e. private
 * to ybc.c compilation unit).
 ******************************************************************************/

#ifdef YBC_PLATFORM_LINUX
  /*
   * Required for certain linux-specific extensions such as O_NOATIME flag
   * in open() syscall.
   * This macro must be defined before any #include's.
   */
  #define _GNU_SOURCE
#endif

#include <stddef.h>     /* size_t */
#include <stdint.h>     /* uint*_t */

/*
 * Allocates the given amount of memory. Always returns non-NULL.
 *
 * Allocated memory must be freed with p_free().
 */
static void *p_malloc(size_t size);

/*
 * Frees memory allocated with either p_malloc() or p_strdup().
 */
static void p_free(void *ptr);

/*
 * Frees memory occupied by *dst and makes a duplicate from src.
 *
 * If src is NULL, then makes *dst NULL.
 *
 * *dst must be freed with p_free().
 */
static void p_strdup(char **dst, const char *src);

/*
 * Returns current time in milliseconds since the Epoch.
 *
 * The caller must be aware that the returned time may jump backwards
 * in the event of NTP adjustment ( http://www.ntp.org/ ) or manual adjustment
 * of system-wide clock.
 */
static uint64_t p_get_current_time(void);

/*
 * Suspends the current thread for the given number of milliseconds.
 */
static void p_sleep(uint64_t milliseconds);

/*
 * Prototype for a function, which can be executed in a thread.
 */
typedef void (*p_thread_func)(void *);

/*
 * Thread structure. Each platform may define arbitrary contents
 * for this structure.
 */
struct p_thread;

/*
 * Initializes thread structure t and starts the given func attached to t.
 * Passes ctx to func.
 */
static void p_thread_init_and_start(struct p_thread *t, p_thread_func func,
    void *ctx);

/*
 * Wait until the given thread t is terminated, then destroys t contents.
 */
static void p_thread_join_and_destroy(struct p_thread *t);

/*
 * Lock structure. Each platform may define arbitrary contents
 * for this structure.
 */
struct p_lock;

/*
 * Initializes lock structure.
 */
static void p_lock_init(struct p_lock *lock);

/*
 * Destroys lock structure.
 */
static void p_lock_destroy(struct p_lock *lock);

/*
 * Locks the given lock.
 */
static void p_lock_lock(struct p_lock *lock);

/*
 * Unlocks the given lock.
 */
static void p_lock_unlock(struct p_lock *lock);

/*
 * Event structure. Each platform may define arbitrary contents
 * for this structure.
 */
struct p_event;

/*
 * Initializes the given event structure.
 */
static void p_event_init(struct p_event *e);

/*
 * Destroys the given event structure.
 */
static void p_event_destroy(struct p_event *e);

/*
 * Sets the given event to 'alerted' state.
 *
 * This wakes up all the threads blocked on this event
 * in p_event_wait_with_timeout().
 */
static void p_event_set(struct p_event *e);

/*
 * Suspends the current thread until the given event is set to 'alerted' state.
 *
 * Returns 1 if the event was set to 'alerted' state. Returns 0 if the event
 * wasn't set to 'alerted' state during timeout milliseconds.
 */
static int p_event_wait_with_timeout(struct p_event *e, uint64_t timeout);

/*
 * File structure. Each platform may define arbitrary contents
 * for this structure.
 */
struct p_file;

/*
 * Creates an anonymous temporary file, which will be automatically deleted
 * after the file is closed.
 */
static void p_file_create_anonymous(struct p_file *file);

/*
 * Checks whether a file with the given filename exists.
 *
 * Return 1 if the file exists, otherwise returns 0.
 */
static int p_file_exists(const char *filename);

/*
 * Creates a file with the given filename.
 */
static void p_file_create(struct p_file *file, const char *filename);

/*
 * Opens a file with the given filename.
 */
static void p_file_open(struct p_file *file, const char *filename);

/*
 * Closes the given file.
 */
static void p_file_close(const struct p_file *file);

/*
 * Removes a file with the given filename.
 */
static void p_file_remove(const char *filename);

/*
 * Returns size of the given file.
 */
static void p_file_get_size(const struct p_file *file, size_t *size);

/*
 * Resizes the file to the given size and makes sure the underlying space
 * in the file is actually allocated (i.e. avoids creating sparse files).
 */
static void p_file_resize_and_preallocate(const struct p_file *file,
    size_t size);

/*
 * Hints the OS about random access pattern to the given file in the range
 * [0...size] bytes.
 */
static void p_file_advise_random_access(const struct p_file *file, size_t size);

/*
 * Tries caching the given file contents in RAM.
 */
static void p_file_cache_in_ram(const struct p_file *file);

/*
 * Initializes memory API.
 *
 * This function must be called first before using other p_memory_* functions.
 * This function may be called multiple times.
 */
static void p_memory_init(void);

/*
 * Returns memory page mask.
 */
static size_t p_memory_page_mask(void);

/*
 * Maps size bytes of the given file into memory and stores memory pointer
 * to *ptr.
 */
static void p_memory_map(void **ptr, const struct p_file *file, size_t size);

/*
 * Unmaps size bytes pointed by ptr from memory.
 */
static void p_memory_unmap(void *ptr, size_t size);

/*
 * Flushes size bytes pointed by ptr to backing storage.
 */
static void p_memory_sync(void *ptr, size_t size);


#ifdef YBC_PLATFORM_LINUX
  #include "platform/linux.c"
#else
  #error "unsupported platform"
#endif

#endif  /* YBC_PLATFORM_H_INCLUDED */
