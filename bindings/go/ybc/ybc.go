// Package ybc provides Go wrapper around YBC library - see
// https://github.com/valyala/ybc .
//
// YBC is intended for creating extremly fast in-process blob caches,
// which can efficiently cache virtually unlimited number of items.
//
// YBC knows how to deal with huge items (i.e. videos, audios, images)
// and caches exceeding available RAM sizes by multiple orders of magnitude.
//
// YBC supports persistent caches surviving application restarts.
//
// YBC is optimized for both HDDs and SSDs.
package ybc

/*
#cgo release CFLAGS: -O2 -DNDEBUG
#cgo linux CFLAGS: -std=gnu99 -DYBC_PLATFORM_LINUX
#cgo linux LDFLAGS: -lrt
#include "ybc_go_glue.c"
#include <stdlib.h> // free
*/
import "C"

import (
	"errors"
	"hash/fnv"
	"io"
	"reflect"
	"time"
	"unsafe"
)

var (
	ErrNoSpace       = errors.New("ybc: not enough space in the cache")
	ErrItemTooLarge  = errors.New("ybc: item is too large to store in the cache")
	ErrCacheMiss     = errors.New("ybc: the item is not found in the cache")
	ErrOpenFailed    = errors.New("ybc: cannot open the cache")
	ErrOutOfRange    = errors.New("ybc: out of range offset")
	ErrPartialCommit = errors.New("ybc: partial commit")
	ErrWouldBlock    = errors.New("ybc: the operation would block")

	// Errors for internal use only
	errPanic = errors.New("ybc: panic")
)

var (
	// Maximum time to live for cached items.
	//
	// Use this value when adding items, which must live in the cache as long
	// as possible.
	MaxTtl = time.Hour * 24 * 365 * 100
)

var (
	configSize = int(C.ybc_config_get_size())
	cacheSize  = int(C.ybc_get_size())
	addTxnSize = int(C.ybc_set_txn_get_size())
	itemSize   = int(C.ybc_item_get_size())
)

// SimpleCache, Cache and Cluster implement this interface
type SimpleCacher interface {
	Set(key []byte, value []byte, ttl time.Duration) error
	Get(key []byte) (value []byte, err error)
	Delete(key []byte) bool
	Clear()
	Close() error
}

// Cache and Cluster implement this interface
type Cacher interface {
	SimpleCacher
	GetDe(key []byte, graceDuration time.Duration) (value []byte, err error)
	GetDeAsync(key []byte, graceDuration time.Duration) (value []byte, err error)
	SetItem(key []byte, value []byte, ttl time.Duration) (item *Item, err error)
	GetItem(key []byte) (item *Item, err error)
	GetDeItem(key []byte, graceDuration time.Duration) (item *Item, err error)
	GetDeAsyncItem(key []byte, graceDuration time.Duration) (item *Item, err error)
	NewSetTxn(key []byte, valueSize int, ttl time.Duration) (txn *SetTxn, err error)
}

/*******************************************************************************
 * Config
 ******************************************************************************/

// Type used in Config for setting big numbers, which may exceed int capacity.
//
// Though the hack with SizeT raises the maximum cache size from 2^31-1
// to 2^63-1, it doesn't help with the maximum cache item size.
// Ybc uses byte slices for represening cache items. So the maximum cache
// item size is limited by the maximum size of a slice. Currently this limit
// is set to 2^31-1 - the maximum value, which can be stored in int type
// on all platforms.
//
// TODO(valyala): substitute SizeT by int after sizeof(int) will become 8
// on 64-bit architectures. Currently amd64's sizeof(int) = 4.
// See http://golang.org/doc/go_faq.html#q_int_sizes for details.
type SizeT uintptr

const (
	// Disables periodic data syncing if assigned to Config.SyncInterval.
	ConfigDisableSync = time.Duration(-1)
)

// Cache configuration.
type Config struct {
	// The maximum number of items the cache can store.
	MaxItemsCount SizeT

	// Cache size (in bytes).
	DataFileSize SizeT

	// Path to index file for the cache.
	//
	// Set this field if you want cache surviving application restarts.
	// The size of index file is proportional to Config.MaxItemsCount.
	//
	// Leave this field empty if you want temporary cache, which is
	// destroyed on application exit.
	IndexFile string

	// Path to data file for the cache.
	//
	// Set this field if you want cache surviving application restarts.
	// The size of data file is equivalent to Config.DataFileSize.
	//
	// Leave this field empty if you want temporary cache, which is
	// destroyed on application exit.
	DataFile string

	// The expected number of hot items in the cache.
	//
	// Setting HotItemsCount to non-zero value enables 'hot items'
	// optimization. This optimization can improve performance for huge
	// caches containing many rarely accessed items (aka 'cold items') and
	// a relatively small number of frequently accessed items
	// (aka 'hot items').
	//
	// Leave this field empty (set to 0) in the following cases:
	//   * if your cache contains less than 1M items.
	//   * if the number of cold items in your cache is comparable to
	//     or smaller than the number of hot items.
	//   * if you are unsure :)
	HotItemsCount SizeT

	// The expected size of hot data in the cache (in bytes).
	//
	// Setting HotDataSize to non-zero value enables 'hot data'
	// optimization. This optimization can improve performance for caches
	// containing many small items, where only a small part of these items
	// are frequently accessed (aka 'hot items'). Setting HotDataSize
	// to value close to summary size of hot items in the cache may improve
	// cache performance.
	//
	// Leave this field empty (set to 0) in the following cases:
	//   * if your cache contains only big items with sizes exceeding 64Kb
	//     (for instance, videos, music files, images, etc.).
	//   * if the number of cold items in your cache is comparable to
	//     or smaller than the number of hot items.
	//   * if you are unsure :)
	HotDataSize SizeT

	// The number of buckets in the hashtable used for tracking items
	// affected by dogpile effect.
	//
	// Tune this value only if you plan using dogpile effect-aware
	// functions. This value should be close to the average number
	// of distinct pending items concurrently affected by dogpile effect.
	//
	// Leave this field empty (set to 0) if you are in doubt.
	//
	// Read more about dogpile effect
	// at https://www.google.com/search?q=dogpile+effect .
	DeHashtableSize int

	// Interval for cache syncing to data file.
	//
	// Items added to the cache are synced to data file with this interval.
	// Non-synced cache items may be lost after the program crash or
	// the operating system crash.
	//
	// Setting SyncInterval to ConfigDisableSync disables data syncing.
	// Even if syncing is disabled, all cache items are persisted
	// on Cache.Close() call.
	//
	// Leave this field empty (set to 0) if you are in doubt.
	SyncInterval time.Duration
}

type configInternal struct {
	buf []byte
	ctx *C.struct_ybc_config
	cg  cacheGuard
}

// Opens Cache.
//
// If force is true, then tries fixing the following non-critical errors
// instead of returning ErrOpenFailed:
//   * creates missing index or data files.
//   * fixes incorrect sizes for index or data files.
//
// Do not open the same cache more than once at the same time!
//
// The returned cache must be closed with cache.Close() call!
// Prefer using defer for closing opened caches:
//
//   cache, err := config.OpenCache(true)
//   if err != nil {
//     log.Fatalf("Error when opening the cache: [%s]", err)
//   }
//   defer cache.Close()
//
func (cfg *Config) OpenCache(force bool) (cache *Cache, err error) {
	return cfg.openCacheInternal(force, false)
}

// Opens SimpleCache.
//
// maxItemSize is the maximum size of object the cache can handle.
// Consider using Cache instead of SimpleCache if you plan storing objects
// in the cache bigger than a few Kb.
//
// If force is true, then tries fixing the following non-critical errors
// instead of returning ErrOpenFailed:
//   * creates missing index or data files.
//   * fixes incorrect sizes for index or data files.
//
// Do not open the same cache more than once at the same time!
//
// The returned cache must be closed with cache.Close() call!
// Prefer using defer for closing opened caches:
//
//   sc, err := config.OpenSimpleCache(true)
//   if err != nil {
//     log.Fatalf("Error when opening the cache: [%s]", err)
//   }
//   defer sc.Close()
//
func (cfg *Config) OpenSimpleCache(maxItemSize int, force bool) (sc *SimpleCache, err error) {
	cache, err := cfg.openCacheInternal(force, true)
	if err != nil {
		return
	}
	sc = &SimpleCache{
		cache:       cache,
		maxItemSize: maxItemSize,
	}
	return
}

func (cfg *Config) openCacheInternal(force, isSimpleCache bool) (cache *Cache, err error) {
	c := cfg.internal(isSimpleCache)
	defer C.ybc_config_destroy(c.ctx)

	c.cg.Acquire()
	err = errPanic
	defer func() {
		if err != nil {
			c.cg.Release()
		}
	}()

	cache = &Cache{
		buf: make([]byte, cacheSize),
		cg:  c.cg,
	}
	mForce := C.int(0)
	if force {
		mForce = 1
	}
	if C.ybc_open(cache.ctx(), c.ctx, mForce) == 0 {
		cache = nil
		err = ErrOpenFailed
		return
	}
	cache.dg.Init()
	err = nil
	return
}

// Removes cache files from filesystem.
func (cfg *Config) RemoveCache() {
	c := cfg.internal(false)
	defer C.ybc_config_destroy(c.ctx)

	C.ybc_remove(c.ctx)
}

func (cfg *Config) internal(isSimpleCache bool) *configInternal {
	c := &configInternal{
		buf: make([]byte, configSize),
	}

	ctx := (*C.struct_ybc_config)(unsafe.Pointer(&c.buf[0]))
	C.ybc_config_init(ctx)

	if cfg.MaxItemsCount != 0 {
		C.ybc_config_set_max_items_count(ctx, C.size_t(cfg.MaxItemsCount))
	}
	if cfg.DataFileSize != 0 {
		C.ybc_config_set_data_file_size(ctx, C.size_t(cfg.DataFileSize))
	}
	if cfg.IndexFile != "" {
		indexFileCStr := C.CString(cfg.IndexFile)
		defer C.free(unsafe.Pointer(indexFileCStr))
		C.ybc_config_set_index_file(ctx, indexFileCStr)
		c.cg.SetIndexFile(cfg.IndexFile)
	}
	if cfg.DataFile != "" {
		dataFileCStr := C.CString(cfg.DataFile)
		defer C.free(unsafe.Pointer(dataFileCStr))
		C.ybc_config_set_data_file(ctx, dataFileCStr)
		c.cg.SetDataFile(cfg.DataFile)
	}
	C.ybc_config_set_hot_items_count(ctx, C.size_t(cfg.HotItemsCount))
	C.ybc_config_set_hot_data_size(ctx, C.size_t(cfg.HotDataSize))
	if cfg.DeHashtableSize != 0 {
		C.ybc_config_set_de_hashtable_size(ctx, C.size_t(cfg.DeHashtableSize))
	}
	if cfg.SyncInterval != 0 {
		syncInterval := cfg.SyncInterval
		if syncInterval == ConfigDisableSync {
			syncInterval = 0
		}
		C.ybc_config_set_sync_interval(ctx, C.uint64_t(syncInterval/time.Millisecond))
	}
	if isSimpleCache {
		C.ybc_config_disable_overwrite_protection(ctx)
	}

	c.ctx = ctx
	return c
}

/*******************************************************************************
 * Simple Cache - cache with simplified interface.
 ******************************************************************************/

// Simple Cache handler.
//
// This is a cache with simplified API comparing to Cache API.
// SimpleCache is optimized for working with small objects (up to 1Kb each).
// The number of small objects doesn't matter.
// Its' performance scales better on multi-CPU systems comparing to Cache.
//
// Consider using Cache instead of SimpleCache for storing large objects such
// as images, audio and video files, since SimpleCache has much higher overhead
// when working with large objects.
//
// Note that objects stored via SimpleCache cannot be read via Cache
// and vice versa.
type SimpleCache struct {
	cache       *Cache
	maxItemSize int
}

// Closes the cache.
//
// All opened caches must be closed with this call!
// Do not close the same cache more than once.
func (sc *SimpleCache) Close() error {
	return sc.cache.Close()
}

// Stores the given (key, value) pair with the given ttl in the cache.
//
// value size shouldn't exceed maxItemSize passed to Config.OpenSimpleCache().
func (sc *SimpleCache) Set(key, value []byte, ttl time.Duration) error {
	sc.cache.dg.CheckLive()
	if len(value) > sc.maxItemSize {
		return ErrItemTooLarge
	}
	var k C.struct_ybc_key
	initKey(&k, key)
	var v C.struct_ybc_value
	initValue(&v, value, ttl)
	if C.ybc_simple_set(sc.cache.ctx(), &k, &v) == 0 {
		return ErrNoSpace
	}
	return nil
}

// Returns value associated with the given key
func (sc *SimpleCache) Get(key []byte) (value []byte, err error) {
	sc.cache.dg.CheckLive()
	var k C.struct_ybc_key
	initKey(&k, key)

	value = make([]byte, sc.maxItemSize)
	v := C.struct_ybc_value{
		ptr:  unsafe.Pointer(&value[0]),
		size: C.size_t(len(value)),
	}
	if C.ybc_simple_get(sc.cache.ctx(), &k, &v) != 1 {
		value = nil
		err = ErrCacheMiss
		return
	}
	value = value[:int(v.size)]
	return
}

// Deletes an item associated with the given key.
//
// Returns true if the given item has been deleted.
// Returns false if there were no item with such key in the cache.
func (sc *SimpleCache) Delete(key []byte) bool {
	return sc.cache.Delete(key)
}

// Clears the cache, i.e. removes all the items stored in the cache.
func (sc *SimpleCache) Clear() {
	sc.cache.Clear()
}

/*******************************************************************************
 * Cache
 ******************************************************************************/

// Cache handler.
//
// It is optimized for working with large objects (up to 2Gb each).
// The number of large objects doesn't matter.
//
// Consider using SimpleCache for storing small objects (up to 1Kb).
// It has better performance scalability on multi-CPU system.
type Cache struct {
	dg  debugGuard
	cg  cacheGuard
	buf []byte
}

// Closes the cache.
//
// All opened caches must be closed with this call!
// Do not close the same cache more than once!
func (cache *Cache) Close() error {
	cache.dg.Close()
	cache.cg.Release()
	C.ybc_close(cache.ctx())
	return nil
}

// Stores value with the given key and the given ttl in the cache.
//
// Do not use this method for storing big values in the cache such as video
// files - use Cache.NewSetTxn() instead.
func (cache *Cache) Set(key []byte, value []byte, ttl time.Duration) error {
	cache.dg.CheckLive()
	var k C.struct_ybc_key
	initKey(&k, key)
	var v C.struct_ybc_value
	initValue(&v, value, ttl)
	if C.ybc_item_set(cache.ctx(), &k, &v) == 0 {
		return ErrNoSpace
	}
	return nil
}

// Returns value associated with the given key from the cache.
//
// Sets err to ErrCacheMiss on cache miss.
//
// Do not use this method for obtaining big values from the cache such as video
// files - use Cache.GetItem() instead.
func (cache *Cache) Get(key []byte) (value []byte, err error) {
	item, err := cache.GetItem(key)
	if err != nil {
		return
	}
	value = item.Value()

	// do not use defer item.Close() for performance reasons
	item.Close()
	return
}

// Returns value associated with the given key from the cache using automatic
// protection against dogpile effect during graceDuration interval.
//
// graceDuration is the expected time required for creating the item if
// it is missing in the cache, i.e. if GetDe() sets err to ErrCacheMiss.
// If the caller found missing item with GetDe() call, it should try creating
// the item and storing it into the cache during graceDuration interval.
//
// Since Cache.GetDe() is slower than Cache.Get(), use it only for items
// vulnerable to dogpile effect.
//
// Do not use this method for obtaining big values from the cache such as video
// files - use Cache.GetDeItem() instead.
func (cache *Cache) GetDe(key []byte, graceDuration time.Duration) (value []byte, err error) {
	item, err := cache.GetDeItem(key, graceDuration)
	if err != nil {
		return
	}
	value = item.Value()

	// do not use defer item.Close() for performance reasons
	item.Close()
	return
}

// The same as Cache.GetDe(), but sets err to ErrWouldBlock instead of waiting
// for the value affected by dogpile effect.
//
// Do not use this method for obtaining big values from the cache such as video
// files - use Cache.GetDeAsyncItem() instead.
func (cache *Cache) GetDeAsync(key []byte, graceDuration time.Duration) (value []byte, err error) {
	item, err := cache.GetDeAsyncItem(key, graceDuration)
	if err != nil {
		return
	}
	value = item.Value()

	// do not use defer item.Close() for performance reasons
	item.Close()
	return
}

// Deletes value associated with the given key from the cache.
//
// Returns true on success, false if there was no such value in the cache.
func (cache *Cache) Delete(key []byte) bool {
	cache.dg.CheckLive()
	var k C.struct_ybc_key
	initKey(&k, key)
	return C.ybc_item_remove(cache.ctx(), &k) != C.int(0)
}

// The same as Cache.Set(), but additionally returns item object associated
// with just addded item.
//
// The returned item must be closed with item.Close() call!
func (cache *Cache) SetItem(key []byte, value []byte, ttl time.Duration) (item *Item, err error) {
	cache.dg.CheckLive()
	item = acquireItem()
	var k C.struct_ybc_key
	initKey(&k, key)
	initValue(&item.value, value, ttl)
	if C.go_set_item_and_value(cache.ctx(), item.ctx(), &k, &item.value) == 0 {
		releaseItem(item)
		err = ErrNoSpace
		return
	}
	item.dg.Init()
	return
}

// The same as Cache.Get(), but returns item instead of item's value.
//
// Sets err to ErrCacheMiss on cache miss.
//
// The returned item must be closed with item.Close() call!
//
// Use this method instead of Cache.Get() for obtaining big values
// from the cache such as video files.
func (cache *Cache) GetItem(key []byte) (item *Item, err error) {
	cache.dg.CheckLive()
	item = acquireItem()
	var k C.struct_ybc_key
	initKey(&k, key)
	if C.go_get_item_and_value(cache.ctx(), item.ctx(), &item.value, &k) == 0 {
		releaseItem(item)
		err = ErrCacheMiss
		return
	}
	item.dg.Init()
	return
}

// The same as Cache.GetDe(), but returns item instead of item's value.
//
// The returned item must be closed with item.Close() call!
//
// Use this method instead of Cache.GetDe() for obtaining big values
// from the cache such as video files.
func (cache *Cache) GetDeItem(key []byte, graceDuration time.Duration) (item *Item, err error) {
	for {
		item, err = cache.GetDeAsyncItem(key, graceDuration)
		if err == ErrWouldBlock {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return
	}
}

// The same as Cache.GetDeAsync(), but returns item instead of item's value.
//
// The returned item must be closed with item.Close() call!
//
// Use this method instead of Cache.GetDeAsync() for obtaining big values
// from the cache such as video files.
func (cache *Cache) GetDeAsyncItem(key []byte, graceDuration time.Duration) (item *Item, err error) {
	cache.dg.CheckLive()
	if graceDuration < 0 {
		graceDuration = 0
	}
	item = acquireItem()
	var k C.struct_ybc_key
	initKey(&k, key)
	mGraceTtl := C.uint64_t(graceDuration / time.Millisecond)
	switch C.go_get_item_and_value_de_async(cache.ctx(), item.ctx(), &item.value, &k, mGraceTtl) {
	case C.YBC_DE_WOULDBLOCK:
		releaseItem(item)
		err = ErrWouldBlock
		return
	case C.YBC_DE_NOTFOUND:
		releaseItem(item)
		err = ErrCacheMiss
		return
	case C.YBC_DE_SUCCESS:
		item.dg.Init()
		return
	}
	panic("unreachable")
}

// Starts new 'set transaction' for storing an item in the cache
// with the given valueSize size, the given ttl and the given key.
//
// Returned txn must be finished with txn.Commit*() or txn.Rollback() calls.
//
// Use this method instead of Cache.Set() for storing big items in the cache
// such as video files.
func (cache *Cache) NewSetTxn(key []byte, valueSize int, ttl time.Duration) (txn *SetTxn, err error) {
	cache.dg.CheckLive()
	checkNonNegative(valueSize)
	if ttl < 0 {
		ttl = 0
	}
	txn = acquireSetTxn()
	var k C.struct_ybc_key
	initKey(&k, key)
	if C.ybc_set_txn_begin(cache.ctx(), txn.ctx(), &k, C.size_t(valueSize), C.uint64_t(ttl/time.Millisecond)) == 0 {
		err = ErrNoSpace
		return
	}
	txn.dg.Init()
	return
}

// Instantly removes all the cache contents.
//
// This method is very fast - its' speed doesn't depend on the number of items
// stored in the cache and on the size of the cache.
func (cache *Cache) Clear() {
	cache.dg.CheckLive()
	C.ybc_clear(cache.ctx())
}

func (cache *Cache) ctx() *C.struct_ybc {
	return (*C.struct_ybc)(unsafe.Pointer(&cache.buf[0]))
}

/*******************************************************************************
 * SetTxn
 ******************************************************************************/

// 'set transaction' handler.
// It is used for efficient storage of big items in the cache such as video
// files.
type SetTxn struct {
	dg             debugGuard
	buf            []byte
	unsafeBufCache []byte
	offset         int
}

// Commits the truncated transaction.
//
// Truncated transaction is partially filled transaction. I.e. its' size
// is smaller than valueSize passed to Cache.NewSetTxn()
func (txn *SetTxn) CommitTruncated() error {
	txn.truncateValue()
	return txn.Commit()
}

// Commits the transaction.
//
// The item appears atomically in the cache after the commit.
func (txn *SetTxn) Commit() (err error) {
	txn.dg.CheckLive()
	buf := txn.unsafeBuf()
	if txn.offset != len(buf) {
		err = ErrPartialCommit
		txn.Rollback()
		return
	}
	C.ybc_set_txn_commit(txn.ctx())
	txn.finish()
	return
}

// Rolls back the transaction.
func (txn *SetTxn) Rollback() {
	txn.dg.CheckLive()
	C.ybc_set_txn_rollback(txn.ctx())
	txn.finish()
}

// io.Writer interface implementation
func (txn *SetTxn) Write(p []byte) (n int, err error) {
	txn.dg.CheckLive()
	buf := txn.unsafeBuf()

	n = copy(buf[txn.offset:], p)
	txn.offset += n
	if n < len(p) {
		err = io.ErrShortWrite
		return
	}
	return
}

// io.ReaderFrom interface implementation
func (txn *SetTxn) ReadFrom(r io.Reader) (n int64, err error) {
	txn.dg.CheckLive()
	var nn int
	buf := txn.unsafeBuf()
	nn, err = io.ReadFull(r, buf[txn.offset:])
	txn.offset += nn
	n = int64(nn)
	return
}

// The same as SetTxn.CommitTruncated(), but additionally returns commited item.
//
// The returned item must be closed with item.Close() call!
func (txn *SetTxn) CommitItemTruncated() (item *Item, err error) {
	txn.truncateValue()
	return txn.CommitItem()
}

// The same as SetTxn.Commit(), but additionally returns commited item.
//
// The returned item must be closed with item.Close() call!
func (txn *SetTxn) CommitItem() (item *Item, err error) {
	txn.dg.CheckLive()
	buf := txn.unsafeBuf()
	if txn.offset != len(buf) {
		err = ErrPartialCommit
		txn.Rollback()
		return
	}
	item = acquireItem()
	C.go_commit_item_and_value(txn.ctx(), item.ctx(), &item.value)
	txn.finish()
	item.dg.Init()
	return
}

func (txn *SetTxn) truncateValue() {
	txn.dg.CheckLive()
	txn.unsafeBufCache = nil
	C.ybc_set_txn_update_value_size(txn.ctx(), C.size_t(txn.offset))
}

func (txn *SetTxn) finish() {
	txn.dg.Close()
	txn.unsafeBufCache = nil
	txn.offset = 0
	releaseSetTxn(txn)
}

func (txn *SetTxn) unsafeBuf() []byte {
	if txn.unsafeBufCache == nil {
		mValue := C.struct_ybc_set_txn_value{}
		C.ybc_set_txn_get_value(txn.ctx(), &mValue)
		txn.unsafeBufCache = newUnsafeSlice(mValue.ptr, int(mValue.size))
	}
	return txn.unsafeBufCache
}

func (txn *SetTxn) ctx() *C.struct_ybc_set_txn {
	return (*C.struct_ybc_set_txn)(unsafe.Pointer(&txn.buf[0]))
}

/*******************************************************************************
 * Item
 ******************************************************************************/

// Cache item.
type Item struct {
	dg         debugGuard
	buf        []byte
	value      C.struct_ybc_value
	offset     int
}

// Closes the item.
//
// Every opened item must be closed only once!
func (item *Item) Close() error {
	item.dg.Close()
	C.ybc_item_release(item.ctx())
	item.value.ptr = nil
	item.value.size = 0
	item.offset = 0
	releaseItem(item)
	return nil
}

// Returns value associated with the item.
//
// Do not use this method for obtaining big values such as video files -
// use io.* interface implementations provided by the Item instead.
func (item *Item) Value() []byte {
	item.dg.CheckLive()
	mValue := &item.value
	return C.GoBytes(mValue.ptr, C.int(mValue.size))
}

// Returns the size of value associated with the item.
func (item *Item) Size() int {
	return int(item.value.size)
}

// Returns the number of bytes remaining to read from the item.
func (item *Item) Available() int {
	return item.Size() - item.offset
}

// Returns remaining ttl for the item.
func (item *Item) Ttl() time.Duration {
	item.dg.CheckLive()
	return time.Duration(item.value.ttl) * time.Millisecond
}

// io.Seeker interface implementation
func (item *Item) Seek(offset int64, whence int) (ret int64, err error) {
	bufSize := int64(len(item.unsafeBuf()))
	switch whence {
	case 0:
	case 1:
		offset += int64(item.offset)
	case 2:
		offset += bufSize
	default:
		panic("unsupported whence")
	}
	if offset > bufSize || offset < 0 {
		err = ErrOutOfRange
		return
	}
	item.offset = int(offset)
	ret = offset
	return
}

// io.ByteReader interface implementation
func (item *Item) ReadByte() (c byte, err error) {
	buf := item.unsafeBuf()
	if item.offset == len(buf) {
		err = io.EOF
		return
	}
	c = buf[item.offset]
	item.offset++
	return
}

// io.Reader interface implementation
func (item *Item) Read(p []byte) (n int, err error) {
	buf := item.unsafeBuf()
	n = copy(p, buf[item.offset:])
	item.offset += n
	if n < len(p) {
		err = io.EOF
		return
	}
	return
}

// io.ReaderAt interface implementation
func (item *Item) ReadAt(p []byte, offset int64) (n int, err error) {
	buf := item.unsafeBuf()
	if offset > int64(len(buf)) {
		err = ErrOutOfRange
		return
	}
	n = copy(p, buf[offset:])
	if n < len(p) {
		err = io.EOF
		return
	}
	return
}

// io.WriterTo interface implementation
func (item *Item) WriteTo(w io.Writer) (n int64, err error) {
	var nn int
	buf := item.unsafeBuf()
	nn, err = w.Write(buf[item.offset:])
	item.offset += nn
	n = int64(nn)
	return
}

func (item *Item) unsafeBuf() []byte {
	item.dg.CheckLive()
	mValue := &item.value
	return newUnsafeSlice(mValue.ptr, int(mValue.size))
}

func (item *Item) ctx() *C.struct_ybc_item {
	return (*C.struct_ybc_item)(unsafe.Pointer(&item.buf[0]))
}

/*******************************************************************************
 * ClusterConfig
 ******************************************************************************/

// Configuration required for opening a Cluster.
type ClusterConfig []*Config

// Opens a cluster of caches.
//
// Tries fixing the following errors if force is set to true:
//   * creating missing index and data files;
//   * adjusting invalid sizes for index and data files.
//
// Cluster of caches may work faster than a single Cache only if the following
// conditions are met:
//   * The total size of frequently accessed items in the cluster exceeds
//     available RAM size.
//   * Backing files for distinct caches in the cluster are located on distinct
//     physical storages.
//
// The returned cluster must be closed with cluster.Close() call!
//
// Do not open the same cluster more than once at the same time!
func (cfg ClusterConfig) OpenCluster(force bool) (cluster *Cluster, err error) {
	cachesCount := len(cfg)
	openedCachesCount := 0
	caches := make([]*Cache, cachesCount)
	defer func() {
		if openedCachesCount < cachesCount {
			for i := 0; i < openedCachesCount; i++ {
				caches[i].Close()
			}
			cluster = nil
		}
	}()

	slotsCount := SizeT(0)
	maxSlotIndexes := make([]SizeT, cachesCount)
	for i := 0; i < cachesCount; i++ {
		caches[i], err = cfg[i].OpenCache(force)
		if err != nil {
			return
		}
		openedCachesCount++
		slotsCount += cfg[i].MaxItemsCount
		maxSlotIndexes[i] = slotsCount
	}

	cluster = &Cluster{
		caches:         caches,
		slotsCount:     slotsCount,
		maxSlotIndexes: maxSlotIndexes,
	}
	cluster.dg.Init()
	return
}

// Removes all files associated with the cluster.
func (cfg ClusterConfig) RemoveCluster() {
	for _, c := range cfg {
		c.RemoveCache()
	}
}

/*******************************************************************************
 * Cluster
 ******************************************************************************/

// Cluster of caches.
type Cluster struct {
	dg             debugGuard
	caches         []*Cache
	slotsCount     SizeT
	maxSlotIndexes []SizeT
}

// Closes the cluster.
//
// Each opened cluster must be closed only once!
func (cluster *Cluster) Close() error {
	cluster.dg.Close()
	cachesCount := len(cluster.caches)
	for i := 0; i < cachesCount; i++ {
		cluster.caches[i].Close()
	}
	return nil
}

// See Cache.Set()
func (cluster *Cluster) Set(key []byte, value []byte, ttl time.Duration) error {
	return cluster.cache(key).Set(key, value, ttl)
}

// See Cache.Get()
func (cluster *Cluster) Get(key []byte) (value []byte, err error) {
	return cluster.cache(key).Get(key)
}

// See Cache.GetDe()
func (cluster *Cluster) GetDe(key []byte, graceDuration time.Duration) (value []byte, err error) {
	return cluster.cache(key).GetDe(key, graceDuration)
}

// See Cache.GetDeAsync()
func (cluster *Cluster) GetDeAsync(key []byte, graceDuration time.Duration) (value []byte, err error) {
	return cluster.cache(key).GetDeAsync(key, graceDuration)
}

// See Cache.Delete()
func (cluster *Cluster) Delete(key []byte) bool {
	return cluster.cache(key).Delete(key)
}

// See Cache.SetItem()
func (cluster *Cluster) SetItem(key []byte, value []byte, ttl time.Duration) (item *Item, err error) {
	return cluster.cache(key).SetItem(key, value, ttl)
}

// See Cache.GetItem()
func (cluster *Cluster) GetItem(key []byte) (item *Item, err error) {
	return cluster.cache(key).GetItem(key)
}

// See Cache.GetDeItem()
func (cluster *Cluster) GetDeItem(key []byte, graceDuration time.Duration) (item *Item, err error) {
	return cluster.cache(key).GetDeItem(key, graceDuration)
}

// See Cache.GetDeAsyncItem()
func (cluster *Cluster) GetDeAsyncItem(key []byte, graceDuration time.Duration) (item *Item, err error) {
	return cluster.cache(key).GetDeAsyncItem(key, graceDuration)
}

// See Cache.NewSetTxn()
func (cluster *Cluster) NewSetTxn(key []byte, valueSize int, ttl time.Duration) (txn *SetTxn, err error) {
	return cluster.cache(key).NewSetTxn(key, valueSize, ttl)
}

// See Cache.Clear()
func (cluster *Cluster) Clear() {
	for _, cache := range cluster.caches {
		cache.Clear()
	}
}

func (cluster *Cluster) cache(key []byte) *Cache {
	cluster.dg.CheckLive()
	h := fnv.New64a()
	h.Write(key)
	idx := SizeT(h.Sum64()) % cluster.slotsCount

	maxSlotIndexes := cluster.maxSlotIndexes
	i := 0
	for idx >= maxSlotIndexes[i] {
		i++
	}
	return cluster.caches[i]
}

/*******************************************************************************
 * Aux functions
 ******************************************************************************/

func initKey(k *C.struct_ybc_key, key []byte) {
	var ptr unsafe.Pointer
	if len(key) > 0 {
		ptr = unsafe.Pointer(&key[0])
	}
	k.ptr = ptr
	k.size = C.size_t(len(key))
}

func initValue(v *C.struct_ybc_value, value []byte, ttl time.Duration) {
	if ttl < 0 {
		ttl = 0
	}
	var ptr unsafe.Pointer
	if len(value) > 0 {
		ptr = unsafe.Pointer(&value[0])
	}
	v.ptr = ptr
	v.size = C.size_t(len(value))
	v.ttl = C.uint64_t(ttl / time.Millisecond)
}

func newUnsafeSlice(ptr unsafe.Pointer, size int) (buf []byte) {
	// This trick is stolen from http://code.google.com/p/go-wiki/wiki/cgo .
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	hdr.Data = uintptr(ptr)
	hdr.Len = size
	hdr.Cap = size
	return
}

/*******************************************************************************
 * Leaky buffers for SetTxn and Item.
 *
 * See http://golang.org/doc/effective_go.html#leaky_buffer .
 ******************************************************************************/

const addTxnsPoolSize = 1024

var addTxnsPool = make(chan *SetTxn, addTxnsPoolSize)

func acquireSetTxn() *SetTxn {
	select {
	case txn := <-addTxnsPool:
		return txn
	default:
		return &SetTxn{
			buf: make([]byte, addTxnSize),
		}
	}
}

func releaseSetTxn(txn *SetTxn) {
	select {
	case addTxnsPool <- txn:
	default:
	}
}

const itemsPoolSize = 1024

var itemsPool = make(chan *Item, itemsPoolSize)

func acquireItem() *Item {
	select {
	case item := <-itemsPool:
		return item
	default:
		return &Item{
			buf: make([]byte, itemSize),
		}
	}
}

func releaseItem(item *Item) {
	select {
	case itemsPool <- item:
	default:
	}
}
