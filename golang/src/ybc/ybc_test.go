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

func newTestConfig() *Config {
	return NewConfig(1000, 1000*1000)
}

func TestNewConfig(t *testing.T) {
	config := newTestConfig()
	defer config.Close()
}

func TestConfig_SetMaxItemsCount(t *testing.T) {
	config := newTestConfig()
	defer config.Close()
	for i := SizeT(1); i < 1000*1000; i *= 100 {
		config.SetMaxItemsCount(i)
	}
}

func TestConfig_SetDataFileSize(t *testing.T) {
	config := newTestConfig()
	defer config.Close()
	for i := SizeT(1); i < 1000*1000; i *= 100 {
		config.SetDataFileSize(i)
	}
}

func TestConfig_SetIndexFile(t *testing.T) {
	config := newTestConfig()
	defer config.Close()
	for i := 1; i < 1000*1000; i *= 100 {
		config.SetIndexFile(fmt.Sprintf("foobar_%d.index", i))
	}
}

func TestConfig_SetDataFile(t *testing.T) {
	config := newTestConfig()
	defer config.Close()
	for i := 1; i < 1000*1000; i *= 100 {
		config.SetDataFile(fmt.Sprintf("foobar_%d.data", i))
	}
}

func TestConfig_SetHotItemsCount(t *testing.T) {
	config := newTestConfig()
	defer config.Close()
	for i := SizeT(1); i < 1000*1000; i *= 100 {
		config.SetHotItemsCount(i)
	}
}

func TestConfig_SetHotDataSize(t *testing.T) {
	config := newTestConfig()
	defer config.Close()
	for i := SizeT(1); i < 1000*1000; i *= 100 {
		config.SetHotDataSize(i)
	}
}

func TestConfig_SetDeHashtableSize(t *testing.T) {
	config := newTestConfig()
	defer config.Close()
	for i := 1; i < 1000*1000; i *= 100 {
		config.SetDeHashtableSize(i)
	}
}

func TestConfig_SetSyncInterval(t *testing.T) {
	config := newTestConfig()
	defer config.Close()
	for i := 1; i < 1000*1000; i *= 100 {
		config.SetSyncInterval(time.Second * time.Duration(i))
	}
}

func TestConfig_RemoveCache_Anonymous(t *testing.T) {
	config := newTestConfig()
	defer config.Close()
	config.RemoveCache()
}

func TestConfig_RemoveCache_Existing(t *testing.T) {
	config := newTestConfig()
	defer config.Close()

	config.SetDataFile("foobar.data")
	config.SetIndexFile("foobar.index")

	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	cache.Close()

	cache, err = config.OpenCache(false)
	if err != nil {
		t.Fatal(err)
	}
	cache.Close()
	config.RemoveCache()

	cache, err = config.OpenCache(false)
	if err != ErrOpenFailed {
		t.Fatal(err)
	}
}

func TestConfig_OpenCache_Anonymous(t *testing.T) {
	config := newTestConfig()
	defer config.Close()

	cache, err := config.OpenCache(false)
	if err != ErrOpenFailed {
		t.Fatal(err)
	}

	for i := 1; i < 10; i++ {
		cache, err = config.OpenCache(true)
		if err != nil {
			t.Fatal(err)
		}
		cache.Close()
	}

	cache, err = config.OpenCache(false)
	if err != ErrOpenFailed {
		t.Fatal(err)
	}
}

func TestConfig_OpenCache_Existing(t *testing.T) {
	config := newTestConfig()
	defer config.Close()

	config.SetDataFile("foobar.data")
	config.SetIndexFile("foobar.index")

	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	defer config.RemoveCache()
	cache.Close()

	for i := 1; i < 10; i++ {
		cache, err := config.OpenCache(false)
		if err != nil {
			t.Fatal(err)
		}
		cache.Close()
	}
}

/*******************************************************************************
 * Cache
 ******************************************************************************/

func newCache(t *testing.T) *Cache {
	config := NewConfig(1000*1000, 10*1000*1000)
	defer config.Close()

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

func TestCache_Add_Get_Remove(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	for i := 1; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.Get(key)
		if err != ErrNotFound {
			t.Fatal(err)
		}
	}

	for i := 1; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err := cache.Add(key, value, MaxTtl)
		if err != nil {
			t.Fatal(err)
		}

		actualValue, err := cache.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		checkValue(t, value, actualValue)
	}

	for i := 1; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		cache.Remove(key)
	}

	for i := 1; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.Get(key)
		if err != ErrNotFound {
			t.Fatal(err)
		}
	}
}

func TestCache_GetDe(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("test")
	_, err := cache.GetDe(key, time.Millisecond*time.Duration(100))
	if err != ErrNotFound {
		t.Fatal(err)
	}

	value := []byte("aaa")
	err = cache.Add(key, value, MaxTtl)
	if err != nil {
		t.Fatal(err)
	}

	actualValue, err := cache.GetDe(key, time.Millisecond*time.Duration(100))
	if err != nil {
		t.Fatal(err)
	}
	checkValue(t, value, actualValue)
}

func TestCache_Clear(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err := cache.Add(key, value, MaxTtl)
		if err != nil {
			t.Fatal(err)
		}
	}

	cache.Clear()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.Get(key)
		if err != ErrNotFound {
			t.Fatal(err)
		}
	}
}

func TestCache_AddItem(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		item, err := cache.AddItem(key, value, MaxTtl)
		if err != nil {
			t.Fatal(err)
		}
		defer item.Close()
		checkValue(t, value, item.Value())
	}
}

func TestCache_GetItem(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.GetItem(key)
		if err != ErrNotFound {
			t.Fatal(err)
		}
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err := cache.Add(key, value, MaxTtl)
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

func TestCache_GetDeItem(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.GetDeItem(key, time.Second)
		if err != ErrNotFound {
			t.Fatal(err)
		}
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err := cache.Add(key, value, MaxTtl)
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

func TestCache_NewAddTxn(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
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

/*******************************************************************************
 * AddTxn
 ******************************************************************************/

func TestAddTxn_Commit(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
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
	if err != ErrNotFound {
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

func TestAddTxn_Commit_Partial(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
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

func TestAddTxn_Rollback(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")

	txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
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
	if err != ErrNotFound {
		t.Fatal(err)
	}
}

func TestAddTxn_CommitItem(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")

	txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
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

func TestAddTxn_ReadFrom(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")

	txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
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
	item, err = cache.AddItem(key, value, MaxTtl)
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

	item, err := cache.AddItem(key, value, MaxTtl)
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

	item, err := cache.AddItem(key, value, MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	defer item.Close()
	if item.Size() != len(value) {
		t.Fatalf("Unexpected size=%d. Expected=%d", item.Size(), len(value))
	}
}

func TestItem_Ttl(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")

	ttl := time.Minute
	item, err := cache.AddItem(key, value, ttl)
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

func newClusterConfig(cachesCount int) *ClusterConfig {
	config := NewClusterConfig(cachesCount)
	for i := 0; i < cachesCount; i++ {
		c := config.Config(i)
		c.SetMaxItemsCount(1000)
		c.SetDataFileSize(1000 * 1000)
	}
	return config
}

func Test_NewClusterConfig(t *testing.T) {
	config := NewClusterConfig(10)
	defer config.Close()
}

func TestClusterConfig_Config(t *testing.T) {
	config := NewClusterConfig(10)
	defer config.Close()

	for i := 0; i < 10; i++ {
		c := config.Config(i)
		c.SetMaxItemsCount(1000)
		c.SetDataFileSize(1000 * 1000)
	}
}

func TestClusterConfig_OpenCluster(t *testing.T) {
	config := newClusterConfig(3)
	defer config.Close()

	_, err := config.OpenCluster(false)
	if err != ErrOpenFailed {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		cluster, err := config.OpenCluster(true)
		if err != nil {
			t.Fatal(err)
		}
		cluster.Close()
	}
}

/*******************************************************************************
 * Cluster
 ******************************************************************************/

func TestCluster_Cache(t *testing.T) {
	config := newClusterConfig(2)
	defer config.Close()
	defer func() {
		for i := 0; i < 2; i++ {
			config.Config(i).RemoveCache()
		}
	}()

	for i := 0; i < 2; i++ {
		c := config.Config(i)
		c.SetDataFile(fmt.Sprintf("cache_%d.data", i))
		c.SetIndexFile(fmt.Sprintf("cache_%d.index", i))
	}

	cluster, err := config.OpenCluster(true)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))
		err := cluster.Cache(key).Add(key, value, MaxTtl)
		if err != nil {
			t.Fatal(err)
		}

		actualValue, err := cluster.Cache(key).Get(key)
		if err != nil {
			t.Fatal(err)
		}
		checkValue(t, value, actualValue)
	}
}
