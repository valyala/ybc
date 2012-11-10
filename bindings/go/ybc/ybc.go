package ybc

// #cgo release CFLAGS: -O2 -DNDEBUG
// #cgo linux CFLAGS: -std=gnu99 -DYBC_PLATFORM_LINUX
// #cgo linux LDFLAGS: -lrt
// #include "ybc.h"
// #include <stdlib.h> // free
import "C"

import (
	"errors"
	"hash/fnv"
	"io"
	"math"
	"reflect"
	"time"
	"unsafe"
)

var (
	// Public errors
	ErrNoSpace           = errors.New("not enough space for the item in the cache")
	ErrCacheMiss         = errors.New("the item is not found in the cache")
	ErrOpenFailed        = errors.New("cannot open the cache")
	ErrOutOfRange        = errors.New("out of range offset")
	ErrPartialCommit     = errors.New("partial commit")
	ErrUnsupportedWhence = errors.New("unsupported whence")
	ErrWouldBlock        = errors.New("the operation would block")

	// Errors for internal use only
	errPanic = errors.New("panic")
)

var (
	MaxTtl = time.Hour * 24 * 365 * 100
)

var (
	configSize = int(C.ybc_config_get_size())
	cacheSize  = int(C.ybc_get_size())
	addTxnSize = int(C.ybc_set_txn_get_size())
	itemSize   = int(C.ybc_item_get_size())
)

/*******************************************************************************
 * Public entities
 ******************************************************************************/

type Cache struct {
	dg  debugGuard
	cg  cacheGuard
	buf []byte
}

type SetTxn struct {
	dg             debugGuard
	buf            []byte
	unsafeBufCache []byte
	offset         int
}

type Item struct {
	dg         debugGuard
	buf        []byte
	valueCache C.struct_ybc_value
	offset     int
}

// Cache and Cluster implement this interface
type Cacher interface {
	Set(key []byte, value []byte, ttl time.Duration) error
	Get(key []byte) (value []byte, err error)
	GetDe(key []byte, graceTtl time.Duration) (value []byte, err error)
	GetDeAsync(key []byte, graceTtl time.Duration) (value []byte, err error)
	Delete(key []byte) bool
	SetItem(key []byte, value []byte, ttl time.Duration) (item *Item, err error)
	GetItem(key []byte) (item *Item, err error)
	GetDeItem(key []byte, graceTtl time.Duration) (item *Item, err error)
	GetDeAsyncItem(key []byte, graceTtl time.Duration) (item *Item, err error)
	NewSetTxn(key []byte, valueSize int, ttl time.Duration) (txn *SetTxn, err error)
	Clear()
}

// TODO: substitute SizeT by int after sizeof(int) will become 8 on 64-bit machines.
// Currently amd64's sizeof(int) = 4. See http://golang.org/doc/go_faq.html#q_int_sizes .
//
// Though the hack with SizeT raises the maximum cache size from 2^31-1 to 2^63-1,
// it doesn't help with the maximum cache item size.
// Ybc uses byte slices for represening cache items. So the maximum cache item size
// is limited by the maximum size of a slice. Currently this limit it set to 2^31-1 -
// the maximum value, which can be stored in int type on all platforms.
type SizeT uintptr

/*******************************************************************************
 * Config
 ******************************************************************************/

const (
	ConfigDisableHotItems = SizeT(math.MaxUint64)
	ConfigDisableHotData  = SizeT(math.MaxUint64)
	ConfigDisableSync     = time.Duration(-1)
)

// Cache configuration.
type Config struct {
	// The number of items the cache can store.
	MaxItemsCount SizeT

	// Cache size in bytes.
	DataFileSize SizeT

	// Path to index file. Leave empty for anonymous cache.
	IndexFile string

	// Path to data file. Leave empty for anonymous cache.
	DataFile string

	// The expected number of hot items in the cache.
	// Set to ConfigDisableHotItems to disable hot items optimization.
	HotItemsCount SizeT

	// The expected size of hot data in bytes.
	// Set to ConfigDisableHotData to disable hot data optimization.
	HotDataSize SizeT

	// The number of buckets in a hashtable used for tracking items affected
	// by dogpile effect.
	DeHashtableSize int

	// Interval for data syncing to files.
	// Set to ConfigDisableSync to disable it.
	SyncInterval time.Duration
}

type configInternal struct {
	buf []byte
	ctx *C.struct_ybc_config
	cg  cacheGuard
}

func (cfg *Config) OpenCache(force bool) (cache *Cache, err error) {
	c := cfg.internal()
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

func (cfg *Config) RemoveCache() {
	c := cfg.internal()
	defer C.ybc_config_destroy(c.ctx)

	C.ybc_remove(c.ctx)
}

func (cfg *Config) internal() *configInternal {
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
	if cfg.HotItemsCount != 0 {
		hotItemsCount := cfg.HotItemsCount
		if hotItemsCount == ConfigDisableHotItems {
			hotItemsCount = 0
		}
		C.ybc_config_set_hot_items_count(ctx, C.size_t(hotItemsCount))
	}
	if cfg.HotDataSize != 0 {
		hotDataSize := cfg.HotDataSize
		if hotDataSize == ConfigDisableHotData {
			hotDataSize = 0
		}
		C.ybc_config_set_hot_data_size(ctx, C.size_t(hotDataSize))
	}
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

	c.ctx = ctx
	return c
}

/*******************************************************************************
 * Cache
 ******************************************************************************/

func (cache *Cache) Close() error {
	cache.dg.Close()
	cache.cg.Release()
	C.ybc_close(cache.ctx())
	return nil
}

func (cache *Cache) Set(key []byte, value []byte, ttl time.Duration) error {
	cache.dg.CheckLive()
	k := newKey(key)
	v := newValue(value, ttl)
	if C.ybc_item_set(cache.ctx(), &k, &v) == 0 {
		return ErrNoSpace
	}
	return nil
}

func (cache *Cache) Get(key []byte) (value []byte, err error) {
	item, err := cache.GetItem(key)
	if err != nil {
		return
	}
	defer item.Close()
	value = item.Value()
	return
}

func (cache *Cache) GetDe(key []byte, graceTtl time.Duration) (value []byte, err error) {
	item, err := cache.GetDeItem(key, graceTtl)
	if err != nil {
		return
	}
	defer item.Close()
	value = item.Value()
	return
}

func (cache *Cache) GetDeAsync(key []byte, graceTtl time.Duration) (value []byte, err error) {
	item, err := cache.GetDeAsyncItem(key, graceTtl)
	if err != nil {
		return
	}
	defer item.Close()
	value = item.Value()
	return
}

func (cache *Cache) Delete(key []byte) bool {
	cache.dg.CheckLive()
	k := newKey(key)
	return C.ybc_item_remove(cache.ctx(), &k) != C.int(0)
}

func (cache *Cache) SetItem(key []byte, value []byte, ttl time.Duration) (item *Item, err error) {
	cache.dg.CheckLive()
	item = acquireItem()
	k := newKey(key)
	v := newValue(value, ttl)
	if C.ybc_item_set_item(cache.ctx(), item.ctx(), &k, &v) == 0 {
		releaseItem(item)
		err = ErrNoSpace
		return
	}
	item.dg.Init()
	return
}

func (cache *Cache) GetItem(key []byte) (item *Item, err error) {
	cache.dg.CheckLive()
	item = acquireItem()
	k := newKey(key)
	if C.ybc_item_get(cache.ctx(), item.ctx(), &k) == 0 {
		releaseItem(item)
		err = ErrCacheMiss
		return
	}
	item.dg.Init()
	return
}

func (cache *Cache) GetDeItem(key []byte, graceTtl time.Duration) (item *Item, err error) {
	for {
		item, err = cache.GetDeAsyncItem(key, graceTtl)
		if err == ErrWouldBlock {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return
	}
	panic("not reachable")
}

func (cache *Cache) GetDeAsyncItem(key []byte, graceTtl time.Duration) (item *Item, err error) {
	cache.dg.CheckLive()
	if graceTtl < 0 {
		graceTtl = 0
	}
	item = acquireItem()
	k := newKey(key)
	mGraceTtl := C.uint64_t(graceTtl / time.Millisecond)
	switch C.ybc_item_get_de_async(cache.ctx(), item.ctx(), &k, mGraceTtl) {
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

func (cache *Cache) NewSetTxn(key []byte, valueSize int, ttl time.Duration) (txn *SetTxn, err error) {
	cache.dg.CheckLive()
	checkNonNegative(valueSize)
	if ttl < 0 {
		ttl = 0
	}
	txn = acquireSetTxn()
	k := newKey(key)
	if C.ybc_set_txn_begin(cache.ctx(), txn.ctx(), &k, C.size_t(valueSize), C.uint64_t(ttl/time.Millisecond)) == 0 {
		err = ErrNoSpace
		return
	}
	txn.dg.Init()
	return
}

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

func (txn *SetTxn) CommitItem() (item *Item, err error) {
	txn.dg.CheckLive()
	buf := txn.unsafeBuf()
	if txn.offset != len(buf) {
		err = ErrPartialCommit
		txn.Rollback()
		return
	}
	item = acquireItem()
	C.ybc_set_txn_commit_item(txn.ctx(), item.ctx())
	txn.finish()
	item.dg.Init()
	return
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

func (item *Item) Close() error {
	item.dg.Close()
	C.ybc_item_release(item.ctx())
	item.valueCache.ptr = nil
	item.offset = 0
	releaseItem(item)
	return nil
}

func (item *Item) Value() []byte {
	item.dg.CheckLive()
	mValue := item.value()
	return C.GoBytes(mValue.ptr, C.int(mValue.size))
}

func (item *Item) Size() int {
	mValue := item.value()
	return int(mValue.size)
}

func (item *Item) Available() int {
	return item.Size() - item.offset
}

func (item *Item) Ttl() time.Duration {
	item.dg.CheckLive()
	return time.Duration(item.value().ttl) * time.Millisecond
}

// io.Seeker interface implementation
func (item *Item) Seek(offset int64, whence int) (ret int64, err error) {
	item.dg.CheckLive()
	if whence != 0 {
		panic(ErrUnsupportedWhence)
	}

	buf := item.unsafeBuf()
	if offset > int64(len(buf)) {
		err = ErrOutOfRange
		return
	}
	item.offset = int(offset)
	ret = offset
	return
}

// io.Reader interface implementation
func (item *Item) Read(p []byte) (n int, err error) {
	item.dg.CheckLive()
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
	item.dg.CheckLive()
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
	item.dg.CheckLive()
	var nn int
	buf := item.unsafeBuf()
	nn, err = w.Write(buf[item.offset:])
	item.offset += nn
	n = int64(nn)
	return
}

func (item *Item) unsafeBuf() []byte {
	mValue := item.value()
	return newUnsafeSlice(mValue.ptr, int(mValue.size))
}

func (item *Item) value() *C.struct_ybc_value {
	if item.valueCache.ptr == nil {
		C.ybc_item_get_value(item.ctx(), &item.valueCache)
	}
	return &item.valueCache
}

func (item *Item) ctx() *C.struct_ybc_item {
	return (*C.struct_ybc_item)(unsafe.Pointer(&item.buf[0]))
}

/*******************************************************************************
 * ClusterConfig
 ******************************************************************************/

type ClusterConfig []*Config

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

func (cfg ClusterConfig) RemoveCluster() {
	for _, c := range cfg {
		c.RemoveCache()
	}
}

/*******************************************************************************
 * Cluster
 ******************************************************************************/

type Cluster struct {
	dg             debugGuard
	caches         []*Cache
	slotsCount     SizeT
	maxSlotIndexes []SizeT
}

func (cluster *Cluster) Close() error {
	cluster.dg.Close()
	cachesCount := len(cluster.caches)
	for i := 0; i < cachesCount; i++ {
		cluster.caches[i].Close()
	}
	return nil
}

func (cluster *Cluster) Set(key []byte, value []byte, ttl time.Duration) error {
	return cluster.cache(key).Set(key, value, ttl)
}

func (cluster *Cluster) Get(key []byte) (value []byte, err error) {
	return cluster.cache(key).Get(key)
}

func (cluster *Cluster) GetDe(key []byte, graceTtl time.Duration) (value []byte, err error) {
	return cluster.cache(key).GetDe(key, graceTtl)
}

func (cluster *Cluster) GetDeAsync(key []byte, graceTtl time.Duration) (value []byte, err error) {
	return cluster.cache(key).GetDeAsync(key, graceTtl)
}

func (cluster *Cluster) Delete(key []byte) bool {
	return cluster.cache(key).Delete(key)
}

func (cluster *Cluster) SetItem(key []byte, value []byte, ttl time.Duration) (item *Item, err error) {
	return cluster.cache(key).SetItem(key, value, ttl)
}

func (cluster *Cluster) GetItem(key []byte) (item *Item, err error) {
	return cluster.cache(key).GetItem(key)
}

func (cluster *Cluster) GetDeItem(key []byte, graceTtl time.Duration) (item *Item, err error) {
	return cluster.cache(key).GetDeItem(key, graceTtl)
}

func (cluster *Cluster) GetDeAsyncItem(key []byte, graceTtl time.Duration) (item *Item, err error) {
	return cluster.cache(key).GetDeAsyncItem(key, graceTtl)
}

func (cluster *Cluster) NewSetTxn(key []byte, valueSize int, ttl time.Duration) (txn *SetTxn, err error) {
	return cluster.cache(key).NewSetTxn(key, valueSize, ttl)
}

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

func newKey(key []byte) C.struct_ybc_key {
	var ptr unsafe.Pointer
	if len(key) > 0 {
		ptr = unsafe.Pointer(&key[0])
	}
	return C.struct_ybc_key{
		ptr:  ptr,
		size: C.size_t(len(key)),
	}
}

func newValue(value []byte, ttl time.Duration) C.struct_ybc_value {
	if ttl < 0 {
		ttl = 0
	}
	var ptr unsafe.Pointer
	if len(value) > 0 {
		ptr = unsafe.Pointer(&value[0])
	}
	return C.struct_ybc_value{
		ptr:  ptr,
		size: C.size_t(len(value)),
		ttl:  C.uint64_t(ttl / time.Millisecond),
	}
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
	panic("unreachable")
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
	panic("unreachable")
}

func releaseItem(item *Item) {
	select {
	case itemsPool <- item:
	default:
	}
}
