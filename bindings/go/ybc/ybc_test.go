package ybc

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"
)

func expectPanic(t *testing.T, f func()) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("unexpected empty panic message")
		}
	}()
	f()
	t.Fatal("the function must panic!")
}

/*******************************************************************************
 * Config
 ******************************************************************************/

func expectOpenCacheSuccess(config *Config, force bool, t *testing.T) {
	cache, err := config.OpenCache(force)
	if err != nil {
		t.Fatalf("cannot open cache: [%s]", err)
	}
	cache.Close()
}

func expectOpenCacheFail(config *Config, force bool, t *testing.T) {
	_, err := config.OpenCache(force)
	if err != ErrOpenFailed {
		t.Fatal("Unexpected error: [%s]", err)
	}
}

func newConfig() *Config {
	return &Config{
		MaxItemsCount: 1000 * 10,
		DataFileSize:  1000 * 1000,
	}
}

func TestConfig_RemoveCache_Anonymous(t *testing.T) {
	config := &Config{}
	config.RemoveCache()
}

func TestConfig_RemoveCache_Existing(t *testing.T) {
	config := newConfig()
	config.DataFile = "foobar.data.remove_existing"
	config.IndexFile = "foobar.index.remove_existing"
	expectOpenCacheSuccess(config, true, t)
	expectOpenCacheSuccess(config, false, t)

	config.RemoveCache()

	expectOpenCacheFail(config, false, t)
}

func TestConfig_OpenCache_Anonymous(t *testing.T) {
	config := newConfig()
	expectOpenCacheFail(config, false, t)
	for i := 1; i < 10; i++ {
		expectOpenCacheSuccess(config, true, t)
	}
	expectOpenCacheFail(config, false, t)
}

func TestConfig_OpenCache_Existing(t *testing.T) {
	config := newConfig()
	config.DataFile = "foobar.data.open_existing"
	config.IndexFile = "foobar.index.open_existing"
	expectOpenCacheSuccess(config, true, t)
	defer config.RemoveCache()

	for i := 1; i < 10; i++ {
		expectOpenCacheSuccess(config, false, t)
	}
}

func TestConfig_OpenCache_EnabledHotItems(t *testing.T) {
	config := newConfig()
	config.HotItemsCount = config.MaxItemsCount / 10
	expectOpenCacheSuccess(config, true, t)
}

func TestConfig_OpenCache_EnabledHotData(t *testing.T) {
	config := newConfig()
	config.HotDataSize = config.DataFileSize / 10
	expectOpenCacheSuccess(config, true, t)
}

func TestConfig_OpenCache_DisabledSync(t *testing.T) {
	config := newConfig()
	config.SyncInterval = ConfigDisableSync
	expectOpenCacheSuccess(config, true, t)
}

/*******************************************************************************
 * Cache
 ******************************************************************************/

func newCache(t *testing.T) *Cache {
	config := newConfig()
	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func checkValue(t *testing.T, expectedValue, actualValue []byte) {
	if bytes.Compare(expectedValue, actualValue) != 0 {
		t.Fatalf("unexpected value: [%s]. Expected [%s]", actualValue, expectedValue)
	}
}

func cacher_Set_Get_Remove(cache Cacher, t *testing.T) {
	defer cache.Close()
	for i := 1; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.Get(key)
		if err != ErrCacheMiss {
			t.Fatal(err)
		}
	}

	for i := 1; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err := cache.Set(key, value, MaxTtl)
		if err != nil {
			t.Fatal(err)
		}

		actualValue, err := cache.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		checkValue(t, value, actualValue)
		if !cache.Delete(key) {
			t.Fatalf("Cannot remove item with key=[%s]", key)
		}
		if cache.Delete(key) {
			t.Fatalf("Unexpected result returned from cache.Delete() for key=[%s]", key)
		}
	}

	for i := 1; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.Get(key)
		if err != ErrCacheMiss {
			t.Fatal(err)
		}
	}
}

func TestCache_Set_Get_Remove(t *testing.T) {
	cache := newCache(t)
	cacher_Set_Get_Remove(cache, t)
}

func cacher_GetDe(cache Cacher, t *testing.T) {
	defer cache.Close()
	key := []byte("test")
	_, err := cache.GetDe(key, time.Millisecond*time.Duration(100))
	if err != ErrCacheMiss {
		t.Fatal(err)
	}
	_, err = cache.GetDeAsync(key, time.Millisecond*time.Duration(100))
	if err != ErrWouldBlock {
		t.Fatal(err)
	}

	value := []byte("aaa")
	err = cache.Set(key, value, MaxTtl)
	if err != nil {
		t.Fatal(err)
	}

	actualValue, err := cache.GetDe(key, time.Millisecond*time.Duration(100))
	if err != nil {
		t.Fatal(err)
	}
	checkValue(t, value, actualValue)
}

func TestCache_GetDe(t *testing.T) {
	cache := newCache(t)
	cacher_GetDe(cache, t)
}

func cacher_Clear(cache Cacher, t *testing.T) {
	defer cache.Close()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err := cache.Set(key, value, MaxTtl)
		if err != nil {
			t.Fatal(err)
		}
	}

	cache.Clear()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.Get(key)
		if err != ErrCacheMiss {
			t.Fatal(err)
		}
	}
}

func TestCache_Clear(t *testing.T) {
	cache := newCache(t)
	cacher_Clear(cache, t)
}

func cacher_SetItem(cache Cacher, t *testing.T) {
	defer cache.Close()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		item, err := cache.SetItem(key, value, MaxTtl)
		if err != nil {
			t.Fatal(err)
		}
		defer item.Close()
		checkValue(t, value, item.Value())
	}
}

func TestCache_SetItem(t *testing.T) {
	cache := newCache(t)
	cacher_SetItem(cache, t)
}

func cacher_GetItem(cache Cacher, t *testing.T) {
	defer cache.Close()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.GetItem(key)
		if err != ErrCacheMiss {
			t.Fatal(err)
		}
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err := cache.Set(key, value, MaxTtl)
		if err != nil {
			t.Fatal(err)
		}

		item, err := cache.GetItem(key)
		if err != nil {
			t.Fatal(err)
		}
		defer item.Close()
		checkValue(t, value, item.Value())
	}
}

func TestCache_GetItem(t *testing.T) {
	cache := newCache(t)
	cacher_GetItem(cache, t)
}

func cacher_GetDeItem(cache Cacher, t *testing.T) {
	defer cache.Close()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.GetDeItem(key, time.Second)
		if err != ErrCacheMiss {
			t.Fatal(err)
		}
		_, err = cache.GetDeAsyncItem(key, time.Second)
		if err != ErrWouldBlock {
			t.Fatal(err)
		}
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err := cache.Set(key, value, MaxTtl)
		if err != nil {
			t.Fatal(err)
		}

		item, err := cache.GetDeItem(key, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer item.Close()
		checkValue(t, value, item.Value())
	}
}

func TestCache_GetDeItem(t *testing.T) {
	cache := newCache(t)
	cacher_GetDeItem(cache, t)
}

func cacher_NewSetTxn(cache Cacher, t *testing.T) {
	defer cache.Close()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
		if err != nil {
			t.Fatal(err)
		}
		n, err := txn.Write(value)
		if err != nil {
			txn.Rollback()
			t.Fatal(err)
		}
		if n != len(value) {
			t.Fatalf("unexpected number of bytes written=%d. Expected %d", n, len(value))
		}
		err = txn.Commit()
		if err != nil {
			t.Fatal(err)
		}

		actualValue, err := cache.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		checkValue(t, value, actualValue)
	}
}

func TestCache_NewSetTxn(t *testing.T) {
	cache := newCache(t)
	cacher_NewSetTxn(cache, t)
}

/*******************************************************************************
 * SetTxn
 ******************************************************************************/

func TestSetTxn_Commit(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	n, err := txn.Write(value)
	if err != nil {
		txn.Rollback()
		t.Fatal(err)
	}
	if n != len(value) {
		txn.Rollback()
		t.Fatalf("unexpected number of bytes written=%d. Expected %d", n, len(value))
	}

	// The item shouldn't exist in the cache before commit
	_, err = cache.Get(key)
	if err != ErrCacheMiss {
		txn.Rollback()
		t.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// The item should appear in the cache after the commit
	actualValue, err := cache.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	checkValue(t, value, actualValue)
}

func TestSetTxn_Commit_Partial(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	n, err := txn.Write(value[:2])
	if err != nil {
		txn.Rollback()
		t.Fatal(err)
	}
	if n != 2 {
		txn.Rollback()
		t.Fatalf("unexpected number of bytes written=%d. Expected %d", n, 2)
	}

	err = txn.Commit()
	if err != ErrPartialCommit {
		t.Fatal(err)
	}
}

func TestSetTxn_Rollback(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")

	txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	n, err := txn.Write(value)
	if err != nil {
		txn.Rollback()
		t.Fatal(err)
	}
	if n != len(value) {
		txn.Rollback()
		t.Fatalf("unexpected number of bytes written=%d. Expected %d", n, len(value))
	}

	txn.Rollback()

	// The item shouldn't exist in the cache after the rollback.
	_, err = cache.Get(key)
	if err != ErrCacheMiss {
		t.Fatal(err)
	}
}

func TestSetTxn_CommitItem(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")

	txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	n, err := txn.Write(value)
	if err != nil {
		txn.Rollback()
		t.Fatal(err)
	}
	if n != len(value) {
		txn.Rollback()
		t.Fatalf("unexpected number of bytes written=%d. Expected %d", n, len(value))
	}

	item, err := txn.CommitItem()
	if err != nil {
		t.Fatal(err)
	}
	defer item.Close()
	checkValue(t, value, item.Value())
}

func TestSetTxn_ReadFrom(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")

	txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}

	valueBuf := bytes.NewBuffer(value)
	n, err := txn.ReadFrom(valueBuf)
	if err != nil {
		txn.Rollback()
		t.Fatal(err)
	}
	if n != int64(len(value)) {
		txn.Rollback()
		t.Fatalf("unexpected number of bytes written=%d. Expected %d", n, len(value))
	}

	item, err := txn.CommitItem()
	if err != nil {
		t.Fatal(err)
	}
	defer item.Close()
	checkValue(t, value, item.Value())
}

/*******************************************************************************
 * Item
 ******************************************************************************/

func newCacheItem(t *testing.T) (cache *Cache, item *Item) {
	cache = newCache(t)

	key := []byte("key")
	value := []byte("value")

	var err error
	item, err = cache.SetItem(key, value, MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestItem_Value(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")

	item, err := cache.SetItem(key, value, MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	defer item.Close()
	checkValue(t, value, item.Value())
}

func TestItem_Size(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")

	item, err := cache.SetItem(key, value, MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	defer item.Close()
	if item.Size() != len(value) {
		t.Fatalf("Unexpected size=%d. Expected=%d", item.Size(), len(value))
	}
}

func TestItem_Available(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key12345")
	value := []byte("value")

	item, err := cache.SetItem(key, value, MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	defer item.Close()

	buf := make([]byte, 3)
	if _, err = item.Read(buf); err != nil {
		t.Fatalf("Cannot read %d bytes from item: [%s]", len(buf), err)
	}
	if item.Available() != (len(value) - len(buf)) {
		t.Fatalf("Unexpected size=%d. Expected=%d", item.Size(), len(value))
	}
}

func TestItem_Ttl(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")

	ttl := time.Minute
	item, err := cache.SetItem(key, value, ttl)
	if err != nil {
		t.Fatal(err)
	}
	defer item.Close()

	if item.Ttl() > ttl {
		t.Fatalf("invalid item's ttl=%s. It cannot be greater than %s", item.Ttl(), ttl)
	}
}

func TestItem_Seek_Read(t *testing.T) {
	cache, item := newCacheItem(t)
	defer cache.Close()
	defer item.Close()

	n, err := item.Seek(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("unexpected n=%d returned in item.Seek(). Expected 2", n)
	}

	value := item.Value()
	buf := make([]byte, 10)
	nn, err := item.Read(buf)
	if nn != len(value)-2 {
		t.Fatalf("unexpected number of bytes read=%d. Expected %d", nn, len(value)-2)
	}
	if err != io.EOF {
		t.Fatal(err)
	}
	checkValue(t, value[2:], buf[:nn])
}

func TestItem_ReadByte(t *testing.T) {
	cache, item := newCacheItem(t)
	defer cache.Close()
	defer item.Close()

	value := item.Value()
	for _, c := range value {
		cc, err := item.ReadByte()
		if err != nil {
			t.Fatalf("unexpected error in Item.ReadByte(): [%s]", err)
		}
		if cc != c {
			t.Fatalf("unexpected byte returned from Item.ReadByte(): %d. Expected %d", cc, c)
		}
	}
	if _, err := item.ReadByte(); err != io.EOF {
		t.Fatalf("Unexpected error returned from Item.ReadByte(): [%s]. Expected EOF", err)
	}
}

func TestItem_Seek_OutOfRange(t *testing.T) {
	cache, item := newCacheItem(t)
	defer cache.Close()
	defer item.Close()

	_, err := item.Seek(100, 0)
	if err != ErrOutOfRange {
		t.Fatal(err)
	}
}

func TestItem_Seek_UnsupportedWhence1(t *testing.T) {
	cache, item := newCacheItem(t)
	defer cache.Close()
	defer item.Close()

	expectPanic(t, func() { item.Seek(100, 1) })
}

func TestItem_Seek_UnsupportedWhence2(t *testing.T) {
	cache, item := newCacheItem(t)
	defer cache.Close()
	defer item.Close()

	expectPanic(t, func() { item.Seek(100, 2) })
}

func TestItem_ReadAt(t *testing.T) {
	cache, item := newCacheItem(t)
	defer cache.Close()
	defer item.Close()

	buf := make([]byte, 2)
	n, err := item.ReadAt(buf, 1)
	if n != len(buf) {
		t.Fatalf("unexpected number of bytes read=%d. Expected %d", n, len(buf))
	}
	if err != nil {
		t.Fatal(err)
	}
	checkValue(t, item.Value()[1:1+n], buf[:n])
}

func TestItem_ReadAt_OutOfRange(t *testing.T) {
	cache, item := newCacheItem(t)
	defer cache.Close()
	defer item.Close()

	buf := make([]byte, 2)
	_, err := item.ReadAt(buf, 100)
	if err != ErrOutOfRange {
		t.Fatal(err)
	}
}

func TestItem_WriteTo(t *testing.T) {
	cache, item := newCacheItem(t)
	defer cache.Close()
	defer item.Close()

	value := item.Value()
	valueBuf := &bytes.Buffer{}
	n, err := item.WriteTo(valueBuf)
	if n != int64(len(value)) {
		t.Fatalf("unexpected number of bytes read=%d. Expected %d", n, len(value))
	}
	if err != nil {
		t.Fatal(err)
	}
	checkValue(t, value, valueBuf.Bytes())
}

/*******************************************************************************
 * ClusterConfig
 ******************************************************************************/

func newClusterConfig(cachesCount int) ClusterConfig {
	cfg := make([]*Config, cachesCount)
	for i := 0; i < cachesCount; i++ {
		cfg[i] = &Config{
			MaxItemsCount: 1000,
			DataFileSize:  1000 * 1000,
		}
	}
	return cfg
}

func TestClusterConfig_OpenCluster(t *testing.T) {
	config := newClusterConfig(3)
	_, err := config.OpenCluster(false)
	if err != ErrOpenFailed {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		cluster, err := config.OpenCluster(true)
		if err != nil {
			t.Fatal(err)
		}
		cluster.Close()
	}
}

func TestClusterConfig_RemoveCluster(t *testing.T) {
	config := ClusterConfig{
		&Config{
			DataFileSize:  1000 * 1000,
			MaxItemsCount: 1000,
			IndexFile:     "cache.index.0",
			DataFile:      "cache.data.0",
		},
		&Config{
			DataFileSize:  1000 * 1000,
			MaxItemsCount: 1000,
			IndexFile:     "cache.index.1",
			DataFile:      "cache.data.1",
		},
	}
	cluster, err := config.OpenCluster(true)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Close()

	config.RemoveCluster()

	_, err = config.OpenCluster(false)
	if err != ErrOpenFailed {
		t.Fatal(err)
	}
}

/*******************************************************************************
 * Cluster
 ******************************************************************************/

func newCluster(t *testing.T) *Cluster {
	config := newClusterConfig(3)
	cluster, err := config.OpenCluster(true)
	if err != nil {
		t.Fatal(err)
	}
	return cluster
}

func TestCluster_Ops(t *testing.T) {
	config := ClusterConfig{
		&Config{
			DataFileSize:  1000 * 1000,
			MaxItemsCount: 1000,
			IndexFile:     "cache.index.0",
			DataFile:      "cache.data.0",
		},
		&Config{
			DataFileSize:  1000 * 1000,
			MaxItemsCount: 1000,
			IndexFile:     "cache.index.1",
			DataFile:      "cache.data.1",
		},
		&Config{
			DataFileSize:  1000 * 1000,
			MaxItemsCount: 1000,
			IndexFile:     "cache.index.2",
			DataFile:      "cache.data.2",
		},
	}
	cluster, err := config.OpenCluster(true)
	if err != nil {
		t.Fatal(err)
	}
	defer config.RemoveCluster()
	defer cluster.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err := cluster.Set(key, value, MaxTtl)
		if err != nil {
			t.Fatal(err)
		}

		actualValue, err := cluster.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		checkValue(t, value, actualValue)
	}
}

func TestCluster_Set_Get_Remove(t *testing.T) {
	cluster := newCluster(t)
	cacher_Set_Get_Remove(cluster, t)
}

func TestCluster_GetDe(t *testing.T) {
	cluster := newCluster(t)
	cacher_GetDe(cluster, t)
}

func TestCluster_Clear(t *testing.T) {
	cluster := newCluster(t)
	cacher_Clear(cluster, t)
}

func TestCluster_SetItem(t *testing.T) {
	cluster := newCluster(t)
	cacher_SetItem(cluster, t)
}

func TestCluster_GetItem(t *testing.T) {
	cluster := newCluster(t)
	cacher_GetItem(cluster, t)
}

func TestCluster_GetDeItem(t *testing.T) {
	cluster := newCluster(t)
	cacher_GetDeItem(cluster, t)
}

func TestCluster_NewSetTxn(t *testing.T) {
	cluster := newCluster(t)
	cacher_NewSetTxn(cluster, t)
}
