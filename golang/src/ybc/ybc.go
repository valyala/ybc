package ybc

// #cgo CFLAGS: -I../../..
// #cgo LDFLAGS: -L../../.. -lybc-debug
// #include <stdlib.h>  // free
// #include "ybc.h"
import "C"

import (
	"errors"
	"io"
	"log"
	"reflect"
	"time"
	"unsafe"
)

var (
	ErrOpenFailed        = errors.New("cannot open the cache")
	ErrNotFound          = errors.New("the item is not found in the cache")
	ErrNoSpace           = errors.New("not enough space for the item in the cache")
	ErrOutOfRange        = errors.New("out of range")
	ErrPartialCommit     = errors.New("partial commit")
	ErrUnsupportedWhence = errors.New("unsupported whence")
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
	buf []byte
}

type Cache struct {
	buf []byte
}

type AddTxn struct {
	buf    []byte
	unsafeBufCache []byte
	offset int
}

type Item struct {
	buf    []byte
	valueCache C.struct_ybc_value
	offset int
}

type ClusterConfig struct {
	buf []byte
}

type Cluster struct {
	buf []byte
}

/*******************************************************************************
 * Config
 ******************************************************************************/

func NewConfig() *Config {
	config := &Config{
		buf: make([]byte, configSize),
	}
	C.ybc_config_init(config.ctx())
	return config
}

func (config *Config) Close() {
	C.ybc_config_destroy(config.ctx())
}

func (config *Config) SetMaxItemsCount(max_items_count uint) {
	C.ybc_config_set_max_items_count(config.ctx(), C.size_t(max_items_count))
}

func (config *Config) SetDataFileSize(data_file_size uint) {
	C.ybc_config_set_data_file_size(config.ctx(), C.size_t(data_file_size))
}

func (config *Config) SetIndexFile(index_file string) {
	c_str := C.CString(index_file)
	defer C.free(unsafe.Pointer(c_str))
	C.ybc_config_set_index_file(config.ctx(), c_str)
}

func (config *Config) SetDataFile(data_file string) {
	c_str := C.CString(data_file)
	defer C.free(unsafe.Pointer(c_str))
	C.ybc_config_set_data_file(config.ctx(), c_str)
}

func (config *Config) SetHotItemsCount(hot_items_count uint) {
	C.ybc_config_set_hot_items_count(config.ctx(), C.size_t(hot_items_count))
}

func (config *Config) SetHotDataSize(hot_data_size uint) {
	C.ybc_config_set_hot_data_size(config.ctx(), C.size_t(hot_data_size))
}

func (config *Config) SetDeHashtableSize(de_hashtable_size uint) {
	C.ybc_config_set_de_hashtable_size(config.ctx(), C.size_t(de_hashtable_size))
}

func (config *Config) SetSyncInterval(sync_interval time.Duration) {
	m_sync_interval := C.uint64_t(sync_interval / time.Millisecond)
	C.ybc_config_set_sync_interval(config.ctx(), m_sync_interval)
}

func (config *Config) RemoveCache() {
	C.ybc_remove(config.ctx())
}

func (config *Config) OpenCache(force bool) (cache *Cache, err error) {
	cache = &Cache{
		buf: make([]byte, cacheSize),
	}
	m_force := C.int(0)
	if force {
		m_force = 1
	}
	if C.ybc_open(cache.ctx(), config.ctx(), m_force) == 0 {
		err = ErrOpenFailed
		return
	}
	return
}

func (config *Config) ctx() *C.struct_ybc_config {
	return (*C.struct_ybc_config)(unsafe.Pointer(&config.buf[0]))
}

/*******************************************************************************
 * Cache
 ******************************************************************************/

func (cache *Cache) Close() {
	C.ybc_close(cache.ctx())
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

func (cache *Cache) GetDe(key []byte, grace_ttl time.Duration) (value []byte, err error) {
	item, err := cache.GetDeItem(key, grace_ttl)
	if err != nil {
		return
	}
	defer item.Close()
	value = item.Value()
	return
}

func (cache *Cache) Remove(key []byte) {
	m_key := newKey(key)
	C.ybc_item_remove(cache.ctx(), m_key)
}

func (cache *Cache) AddItem(key []byte, value []byte, ttl time.Duration) (item *Item, err error) {
	item = newItem()
	m_key := newKey(key)
	m_value := newValue(value, ttl)
	if C.ybc_item_add(cache.ctx(), item.ctx(), m_key, m_value) == 0 {
		err = ErrNoSpace
		return
	}
	return
}

func (cache *Cache) GetItem(key []byte) (item *Item, err error) {
	item = newItem()
	m_key := newKey(key)
	if C.ybc_item_get(cache.ctx(), item.ctx(), m_key) == 0 {
		err = ErrNotFound
		return
	}
	return
}

func (cache *Cache) GetDeItem(key []byte, grace_ttl time.Duration) (item *Item, err error) {
	item = newItem()
	m_key := newKey(key)
	m_grace_ttl := C.uint64_t(grace_ttl / time.Millisecond)
	for {
		switch C.ybc_item_get_de_async(cache.ctx(), item.ctx(), m_key, m_grace_ttl) {
		case C.YBC_DE_WOULDBLOCK:
			time.Sleep(time.Millisecond * 100)
			continue
		case C.YBC_DE_NOTFOUND:
			err = ErrNotFound
			return
		case C.YBC_DE_SUCCESS:
			return
		}
	}
	panic("unreachable")
}

func (cache *Cache) NewAddTxn(key []byte, value_size uint, ttl time.Duration) (txn *AddTxn, err error) {
	txn = &AddTxn{
		buf: make([]byte, addTxnSize),
	}

	m_key := newKey(key)
	m_ttl := C.uint64_t(ttl / time.Millisecond)
	if C.ybc_add_txn_begin(cache.ctx(), txn.ctx(), m_key, C.size_t(value_size), m_ttl) == 0 {
		err = ErrNoSpace
		return
	}
	return
}

func (cache *Cache) Clear() {
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
	C.ybc_add_txn_rollback(txn.ctx())
}

// io.Writer interface implementation
func (txn *AddTxn) Write(p []byte) (n int, err error) {
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
	var nn int
	buf := txn.unsafeBuf()
	nn, err = io.ReadFull(r, buf[txn.offset:])
	txn.offset += nn
	n = int64(nn)
	return
}

func (txn *AddTxn) CommitItem() (item *Item, err error) {
	buf := txn.unsafeBuf()
	if txn.offset != len(buf) {
		err = ErrPartialCommit
		return
	}
	item = newItem()
	C.ybc_add_txn_commit(txn.ctx(), item.ctx())
	return
}

func (txn *AddTxn) unsafeBuf() []byte {
	if txn.unsafeBufCache == nil {
		m_value := C.struct_ybc_add_txn_value{}
		C.ybc_add_txn_get_value(txn.ctx(), &m_value)
		txn.unsafeBufCache = newUnsafeSlice(m_value.ptr, int(m_value.size))
	}
	return txn.unsafeBufCache
}

func (txn *AddTxn) ctx() *C.struct_ybc_add_txn {
	return (*C.struct_ybc_add_txn)(unsafe.Pointer(&txn.buf[0]))
}

/*******************************************************************************
 * Item
 ******************************************************************************/

func (item *Item) Close() {
	C.ybc_item_release(item.ctx())
}

func (item *Item) Value() []byte {
	m_value := item.value()
	return C.GoBytes(m_value.ptr, C.int(m_value.size))
}

func (item *Item) Ttl() time.Duration {
	m_value := item.value()
	return time.Duration(m_value.ttl) * time.Millisecond
}

// io.Seeker interface implementation
func (item *Item) Seek(offset int64, whence int) (ret int64, err error) {
	if whence != 0 {
		err = ErrUnsupportedWhence
		return
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
	m_value := item.value()
	return newUnsafeSlice(m_value.ptr, int(m_value.size))
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

func NewClusterConfig(caches_count int) *ClusterConfig {
	config := &ClusterConfig{
		buf: make([]byte, configSize*caches_count),
	}

	for i := 0; i < caches_count; i++ {
		c := config.Config(i)
		C.ybc_config_init(c.ctx())
	}
	return config
}

func (config *ClusterConfig) Close() {
	for i := 0; i < config.cachesCount(); i++ {
		c := config.Config(i)
		C.ybc_config_destroy(c.ctx())
	}
}

func (config *ClusterConfig) Config(n int) *Config {
	if n < 0 || n >= config.cachesCount() {
		log.Fatal(ErrOutOfRange)
	}
	return &Config{
		buf: config.getConfigBuf(n),
	}
}

func (config *ClusterConfig) OpenCluster(force bool) (cluster *Cluster, err error) {
	cluster = &Cluster{
		buf: make([]byte, C.ybc_cluster_get_size(C.size_t(config.cachesCount()))),
	}
	m_force := C.int(0)
	if force {
		m_force = 1
	}
	if C.ybc_cluster_open(cluster.ctx(), config.ctx(), C.size_t(config.cachesCount()), m_force) == 0 {
		err = ErrOpenFailed
		return
	}
	return
}

func (config *ClusterConfig) ctx() *C.struct_ybc_config {
	return (*C.struct_ybc_config)(unsafe.Pointer(&config.buf[0]))
}

func (config *ClusterConfig) cachesCount() int {
	return len(config.buf) / configSize
}

func (config *ClusterConfig) getConfigBuf(n int) []byte {
	start_idx := n * configSize
	return config.buf[start_idx : start_idx+configSize]
}

/*******************************************************************************
 * Cluster
 ******************************************************************************/

func (cluster *Cluster) Close() {
	C.ybc_cluster_close(cluster.ctx())
}

func (cluster *Cluster) Cache(key []byte) *Cache {
	m_key := newKey(key)
	ctx := C.ybc_cluster_get_cache(cluster.ctx(), m_key)
	return &Cache{
		buf: newUnsafeSlice(unsafe.Pointer(ctx), cacheSize),
	}
}

func (cluster *Cluster) ctx() *C.struct_ybc_cluster {
	return (*C.struct_ybc_cluster)(unsafe.Pointer(&cluster.buf[0]))
}

/*******************************************************************************
 * Aux functions
 ******************************************************************************/

func newKey(key []byte) *C.struct_ybc_key {
	if len(key) == 0 {
		return &C.struct_ybc_key{}
	}
	return &C.struct_ybc_key{
		ptr:  unsafe.Pointer(&key[0]),
		size: C.size_t(len(key)),
	}
}

func newValue(value []byte, ttl time.Duration) *C.struct_ybc_value {
	if len(value) == 0 {
		return &C.struct_ybc_value{}
	}
	return &C.struct_ybc_value{
		ptr:  unsafe.Pointer(&value[0]),
		size: C.size_t(len(value)),
		ttl:  C.uint64_t(ttl / time.Millisecond),
	}
}

func newItem() *Item {
	return &Item{
		buf: make([]byte, itemSize),
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
