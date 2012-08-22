package ybc

// #cgo !release LDFLAGS: -lybc-debug
// #cgo release LDFLAGS: -lybc-release
// #include "ybc.h"
// #include <stdlib.h> // free
import "C"

import (
	"errors"
	"io"
	"reflect"
	"time"
	"unsafe"
)

var (
	// Public errors
	ErrOpenFailed        = errors.New("cannot open the cache")
	ErrNotFound          = errors.New("the item is not found in the cache")
	ErrNoSpace           = errors.New("not enough space for the item in the cache")
	ErrOutOfRange        = errors.New("out of range")
	ErrPartialCommit     = errors.New("partial commit")
	ErrUnsupportedWhence = errors.New("unsupported whence")

	// Errors for internal use only
	errPanic = errors.New("panic")
)

var (
	MaxTtl = time.Hour * 24 * 365 * 100
)

var (
	configSize = int(C.ybc_config_get_size())
	cacheSize  = int(C.ybc_get_size())
	addTxnSize = int(C.ybc_add_txn_get_size())
	itemSize   = int(C.ybc_item_get_size())
)

/*******************************************************************************
 * Public entities
 ******************************************************************************/

type Config struct {
	dg  debugGuard
	cg  cacheGuard
	buf []byte
}

type Cache struct {
	dg  debugGuard
	cg  cacheGuard
	buf []byte
}

type AddTxn struct {
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

type ClusterConfig struct {
	dg           debugGuard
	buf          []byte
	configsCache []*Config
}

type Cluster struct {
	dg          debugGuard
	ccg         clusterCacheGuard
	buf         []byte
	cachesCache map[uintptr]*Cache
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

func NewConfig(maxItemsCount, dataFileSize SizeT) *Config {
	config := newConfig(make([]byte, configSize))
	config.SetMaxItemsCount(maxItemsCount)
	config.SetDataFileSize(dataFileSize)
	return config
}

func (config *Config) Close() error {
	config.dg.CheckLive()
	C.ybc_config_destroy(config.ctx())
	config.dg.SetClosed()
	return nil
}

func (config *Config) SetMaxItemsCount(maxItemsCount SizeT) {
	config.dg.CheckLive()
	C.ybc_config_set_max_items_count(config.ctx(), C.size_t(maxItemsCount))
}

func (config *Config) SetDataFileSize(dataFileSize SizeT) {
	config.dg.CheckLive()
	C.ybc_config_set_data_file_size(config.ctx(), C.size_t(dataFileSize))
}

func (config *Config) SetIndexFile(indexFile string) {
	config.dg.CheckLive()
	config.cg.SetIndexFile(indexFile)
	cStr := C.CString(indexFile)
	defer C.free(unsafe.Pointer(cStr))
	C.ybc_config_set_index_file(config.ctx(), cStr)
}

func (config *Config) SetDataFile(dataFile string) {
	config.dg.CheckLive()
	config.cg.SetDataFile(dataFile)
	cStr := C.CString(dataFile)
	defer C.free(unsafe.Pointer(cStr))
	C.ybc_config_set_data_file(config.ctx(), cStr)
}

func (config *Config) SetHotItemsCount(hotItemsCount SizeT) {
	config.dg.CheckLive()
	C.ybc_config_set_hot_items_count(config.ctx(), C.size_t(hotItemsCount))
}

func (config *Config) SetHotDataSize(hotDataSize SizeT) {
	config.dg.CheckLive()
	C.ybc_config_set_hot_data_size(config.ctx(), C.size_t(hotDataSize))
}

func (config *Config) SetDeHashtableSize(deHashtableSize int) {
	config.dg.CheckLive()
	checkNonNegative(deHashtableSize)
	C.ybc_config_set_de_hashtable_size(config.ctx(), C.size_t(deHashtableSize))
}

func (config *Config) SetSyncInterval(syncInterval time.Duration) {
	config.dg.CheckLive()
	checkNonNegativeDuration(syncInterval)
	C.ybc_config_set_sync_interval(config.ctx(), C.uint64_t(syncInterval/time.Millisecond))
}

func (config *Config) RemoveCache() {
	config.dg.CheckLive()
	C.ybc_remove(config.ctx())
}

func (config *Config) OpenCache(force bool) (cache *Cache, err error) {
	config.dg.CheckLive()
	config.cg.Acquire()
	err = errPanic
	defer func() {
		if err != nil {
			config.cg.Release()
			cache = nil
		}
	}()
	cache = &Cache{
		buf: make([]byte, cacheSize),
		cg:  config.cg,
	}
	mForce := C.int(0)
	if force {
		mForce = 1
	}
	if C.ybc_open(cache.ctx(), config.ctx(), mForce) == 0 {
		err = ErrOpenFailed
		return
	}
	cache.dg.Init()
	err = nil
	return
}

func (config *Config) ctx() *C.struct_ybc_config {
	return (*C.struct_ybc_config)(unsafe.Pointer(&config.buf[0]))
}

/*******************************************************************************
 * Cache
 ******************************************************************************/

func (cache *Cache) Close() error {
	cache.dg.CheckLive()
	cache.cg.Release()
	C.ybc_close(cache.ctx())
	cache.dg.SetClosed()
	return nil
}

func (cache *Cache) Add(key []byte, value []byte, ttl time.Duration) error {
	item, err := cache.AddItem(key, value, ttl)
	if err == nil {
		item.Close()
	}
	return err
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

func (cache *Cache) Remove(key []byte) {
	cache.dg.CheckLive()
	k := newKey(key)
	C.ybc_item_remove(cache.ctx(), &k)
}

func (cache *Cache) AddItem(key []byte, value []byte, ttl time.Duration) (item *Item, err error) {
	cache.dg.CheckLive()
	item = acquireItem()
	k := newKey(key)
	v := newValue(value, ttl)
	if C.ybc_item_add(cache.ctx(), item.ctx(), &k, &v) == 0 {
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
		err = ErrNotFound
		return
	}
	item.dg.Init()
	return
}

func (cache *Cache) GetDeItem(key []byte, graceTtl time.Duration) (item *Item, err error) {
	cache.dg.CheckLive()
	checkNonNegativeDuration(graceTtl)
	item = acquireItem()
	k := newKey(key)
	mGraceTtl := C.uint64_t(graceTtl / time.Millisecond)
	for {
		switch C.ybc_item_get_de_async(cache.ctx(), item.ctx(), &k, mGraceTtl) {
		case C.YBC_DE_WOULDBLOCK:
			time.Sleep(time.Millisecond * 100)
			continue
		case C.YBC_DE_NOTFOUND:
			err = ErrNotFound
			return
		case C.YBC_DE_SUCCESS:
			item.dg.Init()
			return
		}
	}
	panic("unreachable")
}

func (cache *Cache) NewAddTxn(key []byte, valueSize int, ttl time.Duration) (txn *AddTxn, err error) {
	cache.dg.CheckLive()
	checkNonNegative(valueSize)
	checkNonNegativeDuration(ttl)
	txn = acquireAddTxn()
	k := newKey(key)
	if C.ybc_add_txn_begin(cache.ctx(), txn.ctx(), &k, C.size_t(valueSize), C.uint64_t(ttl/time.Millisecond)) == 0 {
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
 * AddTxn
 ******************************************************************************/

func (txn *AddTxn) Commit() (err error) {
	var item *Item
	item, err = txn.CommitItem()
	if err != nil {
		return
	}
	item.Close()
	return
}

func (txn *AddTxn) Rollback() {
	txn.dg.CheckLive()
	C.ybc_add_txn_rollback(txn.ctx())
	txn.finish()
}

// io.Writer interface implementation
func (txn *AddTxn) Write(p []byte) (n int, err error) {
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
func (txn *AddTxn) ReadFrom(r io.Reader) (n int64, err error) {
	txn.dg.CheckLive()
	var nn int
	buf := txn.unsafeBuf()
	nn, err = io.ReadFull(r, buf[txn.offset:])
	txn.offset += nn
	n = int64(nn)
	return
}

func (txn *AddTxn) CommitItem() (item *Item, err error) {
	txn.dg.CheckLive()
	buf := txn.unsafeBuf()
	if txn.offset != len(buf) {
		err = ErrPartialCommit
		txn.Rollback()
		return
	}
	item = acquireItem()
	C.ybc_add_txn_commit(txn.ctx(), item.ctx())
	txn.finish()
	item.dg.Init()
	return
}

func (txn *AddTxn) finish() {
	txn.dg.SetClosed()
	txn.unsafeBufCache = nil
	txn.offset = 0
	releaseAddTxn(txn)
}

func (txn *AddTxn) unsafeBuf() []byte {
	if txn.unsafeBufCache == nil {
		mValue := C.struct_ybc_add_txn_value{}
		C.ybc_add_txn_get_value(txn.ctx(), &mValue)
		txn.unsafeBufCache = newUnsafeSlice(mValue.ptr, int(mValue.size))
	}
	return txn.unsafeBufCache
}

func (txn *AddTxn) ctx() *C.struct_ybc_add_txn {
	return (*C.struct_ybc_add_txn)(unsafe.Pointer(&txn.buf[0]))
}

/*******************************************************************************
 * Item
 ******************************************************************************/

func (item *Item) Close() error {
	item.dg.CheckLive()
	C.ybc_item_release(item.ctx())
	item.dg.SetClosed()
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

func NewClusterConfig(cachesCount int) *ClusterConfig {
	config := &ClusterConfig{
		buf: make([]byte, configSize*cachesCount),
	}
	configsCache := make([]*Config, cachesCount)
	for i := 0; i < cachesCount; i++ {
		configsCache[i] = newConfig(config.configBuf(i))
	}
	config.configsCache = configsCache
	config.dg.Init()
	return config
}

func (config *ClusterConfig) Close() error {
	config.dg.CheckLive()
	for _, c := range config.configsCache {
		c.Close()
	}
	config.dg.SetClosed()
	return nil
}

// DO NOT close the returned config! Its' lifetime is automatically managed
// by the ClusterConfig object.
func (config *ClusterConfig) Config(cacheIndex int) *Config {
	config.dg.CheckLive()
	return config.configsCache[cacheIndex]
}

func (config *ClusterConfig) OpenCluster(force bool) (cluster *Cluster, err error) {
	config.dg.CheckLive()
	ccg := debugAcquireClusterCache(config.configsCache)
	err = errPanic
	defer func() {
		if err != nil {
			debugReleaseClusterCache(ccg)
			cluster = nil
		}
	}()
	cachesCount := len(config.configsCache)
	cluster = &Cluster{
		buf: make([]byte, C.ybc_cluster_get_size(C.size_t(cachesCount))),
		ccg: ccg,
	}
	cluster.cachesCache = make(map[uintptr]*Cache, cachesCount)
	mForce := C.int(0)
	if force {
		mForce = 1
	}
	if C.ybc_cluster_open(cluster.ctx(), config.ctx(), C.size_t(cachesCount), mForce) == 0 {
		err = ErrOpenFailed
		return
	}
	cluster.dg.Init()
	err = nil
	return
}

func (config *ClusterConfig) ctx() *C.struct_ybc_config {
	return (*C.struct_ybc_config)(unsafe.Pointer(&config.buf[0]))
}

func (config *ClusterConfig) configBuf(n int) []byte {
	start_idx := n * configSize
	return config.buf[start_idx : start_idx+configSize]
}

/*******************************************************************************
 * Cluster
 ******************************************************************************/

func (cluster *Cluster) Close() error {
	cluster.dg.CheckLive()
	debugReleaseClusterCache(cluster.ccg)
	C.ybc_cluster_close(cluster.ctx())
	cluster.dg.SetClosed()
	return nil
}

// DO NOT close the returned cache! Its' lifetime is automatically managed
// by the Cluster object.
func (cluster *Cluster) Cache(key []byte) *Cache {
	cluster.dg.CheckLive()
	k := newKey(key)
	p := unsafe.Pointer(C.ybc_cluster_get_cache(cluster.ctx(), &k))
	cache := cluster.cachesCache[uintptr(p)]
	if cache == nil {
		cache = &Cache{
			buf: newUnsafeSlice(p, cacheSize),
		}
		cache.dg.InitNoClose()
		cluster.cachesCache[uintptr(p)] = cache
	}
	return cache
}

func (cluster *Cluster) ctx() *C.struct_ybc_cluster {
	return (*C.struct_ybc_cluster)(unsafe.Pointer(&cluster.buf[0]))
}

/*******************************************************************************
 * Aux functions
 ******************************************************************************/

func newConfig(buf []byte) *Config {
	config := &Config{
		buf: buf,
	}
	C.ybc_config_init(config.ctx())
	config.dg.Init()
	return config
}

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
	checkNonNegativeDuration(ttl)
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
 * Leaky buffers for AddTxn and Item.
 *
 * See http://golang.org/doc/effective_go.html#leaky_buffer .
 ******************************************************************************/

const addTxnsPoolSize = 1024

var addTxnsPool = make(chan *AddTxn, addTxnsPoolSize)

func acquireAddTxn() *AddTxn {
	select {
	case txn := <-addTxnsPool:
		return txn
	default:
		return &AddTxn{
			buf: make([]byte, addTxnSize),
		}
	}
	panic("unreachable")
}

func releaseAddTxn(txn *AddTxn) {
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
