package ybc

// #cgo CFLAGS: -I../../..
// #cgo LDFLAGS: -L../../.. -lybc-debug
// #include <stdlib.h>  // free
// #include "ybc.h"
import "C"

import (
	"errors"
	"io"
	"reflect"
	"time"
	"unsafe"
)

var (
	ErrOpenFailed = errors.New("cannot open the cache")
	ErrNotFound   = errors.New("the item is not found in the cache")
	ErrNoSpace    = errors.New("not enough space for the item in the cache")
	ErrOutOfRange = errors.New("index out of range")
)

var (
	MaxTtl = time.Hour * 24 * 365 * 100
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
	buf []byte
}

type Item struct {
	buf []byte
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

func NewConfig() Config {
	config := Config{
		buf: make([]byte, C.ybc_config_get_size()),
	}
	C.ybc_config_init(config.ctx())
	return config
}

func (config Config) Close() {
	C.ybc_config_destroy(config.ctx())
}

func (config Config) SetMaxItemsCount(max_items_count uint) {
	C.ybc_config_set_max_items_count(config.ctx(), C.size_t(max_items_count))
}

func (config Config) SetDataFileSize(data_file_size uint) {
	C.ybc_config_set_data_file_size(config.ctx(), C.size_t(data_file_size))
}

func (config Config) SetIndexFile(index_file string) {
	c_str := C.CString(index_file)
	defer C.free(unsafe.Pointer(c_str))
	C.ybc_config_set_index_file(config.ctx(), c_str)
}

func (config Config) SetDataFile(data_file string) {
	c_str := C.CString(data_file)
	defer C.free(unsafe.Pointer(c_str))
	C.ybc_config_set_data_file(config.ctx(), c_str)
}

func (config Config) SetHotItemsCount(hot_items_count uint) {
	C.ybc_config_set_hot_items_count(config.ctx(), C.size_t(hot_items_count))
}

func (config Config) SetHotDataSize(hot_data_size uint) {
	C.ybc_config_set_hot_data_size(config.ctx(), C.size_t(hot_data_size))
}

func (config Config) SetDeHashtableSize(de_hashtable_size uint) {
	C.ybc_config_set_de_hashtable_size(config.ctx(), C.size_t(de_hashtable_size))
}

func (config Config) SetSyncInterval(sync_interval time.Duration) {
	m_sync_interval := C.uint64_t(sync_interval / time.Millisecond)
	C.ybc_config_set_sync_interval(config.ctx(), m_sync_interval)
}

func (config Config) RemoveCache() {
	C.ybc_remove(config.ctx())
}

func (config Config) OpenCache(force bool) (cache Cache, err error) {
	cache = Cache{
		buf: make([]byte, C.ybc_get_size()),
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

func (config Config) ctx() *C.struct_ybc_config {
	return (*C.struct_ybc_config)(unsafe.Pointer(&config.buf[0]))
}

/*******************************************************************************
 * Cache
 ******************************************************************************/

func (cache Cache) Close() {
	C.ybc_close(cache.ctx())
}

func (cache Cache) Add(key []byte, value []byte, ttl time.Duration) error {
	item, err := cache.AddItem(key, value, ttl)
	if err == nil {
		item.Close()
	}
	return err
}

func (cache Cache) Get(key []byte) (value []byte, err error) {
	item, err := cache.GetItem(key)
	if err != nil {
		return
	}
	defer item.Close()
	value = item.Value()
	return
}

func (cache Cache) GetDe(key []byte, grace_ttl time.Duration) (value []byte, err error) {
	item, err := cache.GetDeItem(key, grace_ttl)
	if err != nil {
		return
	}
	defer item.Close()
	value = item.Value()
	return
}

func (cache Cache) Remove(key []byte) {
	m_key := newKey(key)
	C.ybc_item_remove(cache.ctx(), m_key)
}

func (cache Cache) AddItem(key []byte, value []byte, ttl time.Duration) (item Item, err error) {
	item = newItem()
	m_key := newKey(key)
	m_value := newValue(value, ttl)
	if C.ybc_item_add(cache.ctx(), item.ctx(), m_key, m_value) == 0 {
		err = ErrNoSpace
		return
	}
	return
}

func (cache Cache) GetItem(key []byte) (item Item, err error) {
	item = newItem()
	m_key := newKey(key)
	if C.ybc_item_get(cache.ctx(), item.ctx(), m_key) == 0 {
		err = ErrNotFound
		return
	}
	return
}

func (cache Cache) GetDeItem(key []byte, grace_ttl time.Duration) (item Item, err error) {
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

func (cache Cache) NewAddTxn(key []byte, value_size uint, ttl time.Duration) (txn AddTxn, err error) {
	txn = AddTxn{
		buf: make([]byte, C.ybc_add_txn_get_size()),
	}

	m_key := newKey(key)
	m_ttl := C.uint64_t(ttl / time.Millisecond)
	if C.ybc_add_txn_begin(cache.ctx(), txn.ctx(), m_key, C.size_t(value_size), m_ttl) == 0 {
		err = ErrNoSpace
		return
	}
	return
}

func (cache Cache) Clear() {
	C.ybc_clear(cache.ctx())
}

func (cache Cache) ctx() *C.struct_ybc {
	return (*C.struct_ybc)(unsafe.Pointer(&cache.buf[0]))
}

/*******************************************************************************
 * AddTxn
 ******************************************************************************/

func (txn AddTxn) Commit() {
	txn.CommitItem().Close()
}

func (txn AddTxn) Rollback() {
	C.ybc_add_txn_rollback(txn.ctx())
}

// io.ReaderFrom interface implementation
func (txn AddTxn) ReadFrom(r io.Reader) (n int64, err error) {
	n, err = txn.ReadFromTo(r, 0)
	return
}

func (txn AddTxn) ReadFromTo(r io.Reader, off int) (n int64, err error) {
	m_value := C.struct_ybc_add_txn_value{}
	C.ybc_add_txn_get_value(txn.ctx(), &m_value)
	buf := newUnsafeSlice(m_value.ptr, int(m_value.size))

	var nn int
	nn, err = io.ReadFull(r, buf[off:])
	n = int64(nn)
	return
}

func (txn AddTxn) CommitItem() Item {
	item := newItem()
	C.ybc_add_txn_commit(txn.ctx(), item.ctx())
	return item
}

func (txn AddTxn) ctx() *C.struct_ybc_add_txn {
	return (*C.struct_ybc_add_txn)(unsafe.Pointer(&txn.buf[0]))
}

/*******************************************************************************
 * Item
 ******************************************************************************/

func (item Item) Close() {
	C.ybc_item_release(item.ctx())
}

func (item Item) Value() []byte {
	m_value := item.getValue()
	return C.GoBytes(m_value.ptr, C.int(m_value.size))
}

func (item Item) Ttl() time.Duration {
	m_value := item.getValue()
	return time.Duration(m_value.ttl) * time.Millisecond
}

// io.WriterTo interface implementation
func (item Item) WriteTo(w io.Writer) (n int64, err error) {
	n, err = item.WriteToFrom(w, 0)
	return
}

func (item Item) WriteToFrom(w io.Writer, off int) (n int64, err error) {
	m_value := item.getValue()
	buf := newUnsafeSlice(m_value.ptr, int(m_value.size))

	var nn int
	nn, err = w.Write(buf[off:])
	n = int64(nn)
	return
}

func (item Item) getValue() *C.struct_ybc_value {
	m_value := C.struct_ybc_value{}
	C.ybc_item_get_value(item.ctx(), &m_value)
	return &m_value
}

func (item Item) ctx() *C.struct_ybc_item {
	return (*C.struct_ybc_item)(unsafe.Pointer(&item.buf[0]))
}

/*******************************************************************************
 * ClusterConfig
 ******************************************************************************/

func NewClusterConfig(caches_count int) ClusterConfig {
	config := ClusterConfig{
		buf: make([]byte, int(C.ybc_config_get_size())*caches_count),
	}

	for i := 0; i < caches_count; i++ {
		c := config.Config(i)
		C.ybc_config_init(c.ctx())
	}
	return config
}

func (config ClusterConfig) Close() {
	for i := 0; i < config.cachesCount(); i++ {
		c := config.Config(i)
		C.ybc_config_destroy(c.ctx())
	}
}

func (config ClusterConfig) Config(n int) Config {
	if n < 0 || n >= config.cachesCount() {
		panic(ErrOutOfRange)
	}
	return Config{
		buf: config.getConfigBuf(n),
	}
}

func (config ClusterConfig) OpenCluster(force bool) (cluster Cluster, err error) {
	cluster = Cluster{
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

func (config ClusterConfig) ctx() *C.struct_ybc_config {
	return (*C.struct_ybc_config)(unsafe.Pointer(&config.buf[0]))
}

func (config ClusterConfig) cachesCount() int {
	return len(config.buf) / int(C.ybc_config_get_size())
}

func (config ClusterConfig) getConfigBuf(n int) []byte {
	config_size := int(C.ybc_config_get_size())
	start_idx := n * config_size
	return config.buf[start_idx : start_idx+config_size]
}

/*******************************************************************************
 * Cluster
 ******************************************************************************/

func (cluster Cluster) Close() {
	C.ybc_cluster_close(cluster.ctx())
}

func (cluster Cluster) Cache(key []byte) Cache {
	m_key := newKey(key)
	ctx := C.ybc_cluster_get_cache(cluster.ctx(), m_key)
	return Cache{
		buf: newUnsafeSlice(unsafe.Pointer(ctx), int(C.ybc_get_size())),
	}
}

func (cluster Cluster) ctx() *C.struct_ybc_cluster {
	return (*C.struct_ybc_cluster)(unsafe.Pointer(&cluster.buf[0]))
}

/*******************************************************************************
 * Aux functions
 ******************************************************************************/

func newKey(key []byte) *C.struct_ybc_key {
	return &C.struct_ybc_key{
		ptr:  unsafe.Pointer(&key[0]),
		size: C.size_t(len(key)),
	}
}

func newValue(value []byte, ttl time.Duration) *C.struct_ybc_value {
	return &C.struct_ybc_value{
		ptr:  unsafe.Pointer(&value[0]),
		size: C.size_t(len(value)),
		ttl:  C.uint64_t(ttl / time.Millisecond),
	}
}

func newItem() Item {
	return Item{
		buf: make([]byte, C.ybc_item_get_size()),
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
