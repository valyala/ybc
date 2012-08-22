// Tests for development build only

// +build !release

package ybc

import (
	"fmt"
	"testing"
)

/*******************************************************************************
 * Config
 ******************************************************************************/

func TestConfig_DoubleClose(t *testing.T) {
	config := newTestConfig()
	config.Close()
	expectPanic(t, func() { config.Close() })
}

func TestConfig_SameDataIndexFiles(t *testing.T) {
	config := newTestConfig()
	defer config.Close()

	config.SetDataFile("foobar")
	config.SetIndexFile("foobar")

	expectPanic(t, func() { config.OpenCache(true) })
}

func TestConfig_DoubleDataFileOpen(t *testing.T) {
	config := newTestConfig()
	defer config.Close()

	config.SetDataFile("foobar.data")

	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	defer config.RemoveCache()
	defer cache.Close()

	expectPanic(t, func() { config.OpenCache(true) })
}

func TestConfig_DoubleIndexFileOpen(t *testing.T) {
	config := newTestConfig()
	defer config.Close()

	config.SetIndexFile("foobar.index")

	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	defer config.RemoveCache()
	defer cache.Close()

	expectPanic(t, func() { config.OpenCache(true) })
}

func TestConfig_DoubleOpen(t *testing.T) {
	config := newTestConfig()
	defer config.Close()

	config.SetDataFile("foobar.data")
	config.SetIndexFile("foobar.index")

	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	defer config.RemoveCache()
	defer cache.Close()

	expectPanic(t, func() { config.OpenCache(true) })
}

func TestConfig_DoubleOpenViaDistinctConfigs(t *testing.T) {
	config1 := newTestConfig()
	defer config1.Close()

	config1.SetDataFile("foobar.data")
	config1.SetIndexFile("foobar.index")

	cache, err := config1.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	defer config1.RemoveCache()
	defer cache.Close()

	config2 := newTestConfig()
	defer config2.Close()

	config2.SetDataFile("foobar.data")
	config2.SetIndexFile("foobar.index")
	expectPanic(t, func() { config2.OpenCache(true) })
}

/*******************************************************************************
 * Cache
 ******************************************************************************/

func TestCache_DoubleClose(t *testing.T) {
	cache := newCache(t)
	cache.Close()
	expectPanic(t, func() { cache.Close() })
}

/*******************************************************************************
 * AddTxn
 ******************************************************************************/

func TestAddTxn_DoubleCommit(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	txn.Write(value)

	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}
	expectPanic(t, func() { txn.Commit() })
}

func TestAddTxn_DoubleCommitItem(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	txn.Write(value)

	item, err := txn.CommitItem()
	if err != nil {
		t.Fatal(err)
	}
	defer item.Close()
	expectPanic(t, func() { txn.CommitItem() })
}

func TestAddTxn_DoubleRollback(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
	expectPanic(t, func() { txn.Rollback() })
}

func TestAddTxn_CommitAfterRollback(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	txn.Write(value)
	txn.Rollback()
	expectPanic(t, func() { txn.Commit() })
}

func TestAddTxn_RollbackAfterCommit(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewAddTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	txn.Write(value)

	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}
	expectPanic(t, func() { txn.Rollback() })
}

/*******************************************************************************
 * Item
 ******************************************************************************/

func TestItem_DoubleClose(t *testing.T) {
	cache, item := newCacheItem(t)
	defer cache.Close()
	item.Close()
	expectPanic(t, func() { item.Close() })
}

/*******************************************************************************
 * ClusterConfig
 ******************************************************************************/

func TestClusterConfig_DoubleClose(t *testing.T) {
	config := newClusterConfig(10)
	config.Close()
	expectPanic(t, func() { config.Close() })
}

func TestClusterConfig_ConfigClose(t *testing.T) {
	config := newClusterConfig(3)
	defer config.Close()

	c := config.Config(0)
	expectPanic(t, func() { c.Close() })
}

func TestClusterConfig_SameDataIndexFiles(t *testing.T) {
	config := newClusterConfig(3)
	defer config.Close()

	for i := 0; i < 3; i++ {
		c := config.Config(i)
		c.SetDataFile(fmt.Sprintf("foobar.%d", i))
		c.SetIndexFile(fmt.Sprintf("foobar.%d", i))
	}

	expectPanic(t, func() { config.OpenCluster(true) })
}

func TestClusterConfig_DuplicateFiles(t *testing.T) {
	config := newClusterConfig(3)
	defer config.Close()

	for i := 0; i < 3; i++ {
		c := config.Config(i)
		c.SetDataFile("foobar.data")
		c.SetIndexFile("foobar.index")
	}

	expectPanic(t, func() { config.OpenCluster(true) })
}

func TestClusterConfig_DoubleOpen(t *testing.T) {
	config := newClusterConfig(3)
	defer config.Close()
	defer func() {
		for i := 0; i < 3; i++ {
			config.Config(i).RemoveCache()
		}
	}()

	for i := 0; i < 3; i++ {
		c := config.Config(i)
		c.SetDataFile(fmt.Sprintf("foobar.data.%d", i))
		c.SetIndexFile(fmt.Sprintf("foobar.index.%d", i))
	}

	cluster, err := config.OpenCluster(true)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	expectPanic(t, func() { config.OpenCluster(true) })
}

/*******************************************************************************
 * Cluster
 ******************************************************************************/

func TestCluster_DoubleClose(t *testing.T) {
	config := newClusterConfig(2)
	defer config.Close()

	cluster, err := config.OpenCluster(true)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Close()
	expectPanic(t, func() { cluster.Close() })
}

func TestCluster_CacheClose(t *testing.T) {
	config := newClusterConfig(2)
	defer config.Close()

	cluster, err := config.OpenCluster(true)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	cache := cluster.Cache([]byte("test"))
	expectPanic(t, func() { cache.Close() })
}
