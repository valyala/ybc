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

func TestConfig_DoubleDataFileOpen(t *testing.T) {
	config := newConfig()
	config.DataFile = "foobar.data.double_open"
	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	defer config.RemoveCache()
	defer cache.Close()

	expectPanic(t, func() { config.OpenCache(true) })
}

func TestConfig_DoubleIndexFileOpen(t *testing.T) {
	config := newConfig()
	config.IndexFile = "foobar.index.double_open"
	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	defer config.RemoveCache()
	defer cache.Close()

	expectPanic(t, func() { config.OpenCache(true) })
}

func TestConfig_DoubleOpen(t *testing.T) {
	config := newConfig()
	config.DataFile = "foobar.data.double_open2"
	config.IndexFile = "foobar.index.double_open2"
	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	defer config.RemoveCache()
	defer cache.Close()

	expectPanic(t, func() { config.OpenCache(true) })
}

func TestConfig_DoubleOpenViaDistinctConfigs(t *testing.T) {
	config1 := newConfig()
	config1.DataFile = "foobar.data.double_open3"
	config1.IndexFile = "foobar.index.double_open3"
	cache, err := config1.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	defer config1.RemoveCache()
	defer cache.Close()

	config2 := newConfig()
	config2.DataFile = config1.DataFile
	config2.IndexFile = config1.IndexFile
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
 * SetTxn
 ******************************************************************************/

func TestSetTxn_DoubleCommit(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
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

func TestSetTxn_DoubleCommitItem(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
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

func TestSetTxn_DoubleRollback(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	txn.Rollback()
	expectPanic(t, func() { txn.Rollback() })
}

func TestSetTxn_CommitAfterRollback(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
	if err != nil {
		t.Fatal(err)
	}
	txn.Write(value)
	txn.Rollback()
	expectPanic(t, func() { txn.Commit() })
}

func TestSetTxn_RollbackAfterCommit(t *testing.T) {
	cache := newCache(t)
	defer cache.Close()

	key := []byte("key")
	value := []byte("value")
	txn, err := cache.NewSetTxn(key, len(value), MaxTtl)
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

func TestClusterConfig_SameDataIndexFiles(t *testing.T) {
	config := newClusterConfig(3)

	for i := 0; i < 3; i++ {
		c := config[i]
		c.DataFile = fmt.Sprintf("foobar.same_data_index.%d", i)
		c.IndexFile = fmt.Sprintf("foobar.same_data_index.%d", i)
	}

	expectPanic(t, func() { config.OpenCluster(true) })
}

func TestClusterConfig_DuplicateFiles(t *testing.T) {
	config := newClusterConfig(3)

	for i := 0; i < 3; i++ {
		c := config[i]
		c.DataFile = "foobar.data.duplicate_files"
		c.IndexFile = "foobar.index.duplicate_files"
	}
	defer config[0].RemoveCache()

	expectPanic(t, func() { config.OpenCluster(true) })
}

func TestClusterConfig_DoubleOpen(t *testing.T) {
	config := newClusterConfig(3)
	defer config.RemoveCluster()

	for i := 0; i < 3; i++ {
		c := config[i]
		c.DataFile = fmt.Sprintf("foobar.data.cluster_double_open.%d", i)
		c.IndexFile = fmt.Sprintf("foobar.index.cluster_double_open.%d", i)
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
	cluster, err := config.OpenCluster(true)
	if err != nil {
		t.Fatal(err)
	}
	cluster.Close()
	expectPanic(t, func() { cluster.Close() })
}
