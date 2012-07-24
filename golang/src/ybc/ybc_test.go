package ybc

import (
	"fmt"
	"testing"
	"time"
)

func TestNewConfig(t *testing.T) {
	config := NewConfig()
	defer config.Close()
}

func TestConfig_SetMaxItemsCount(t *testing.T) {
	config := NewConfig()
	defer config.Close()
	for i := 1; i < 1000 * 1000; i *= 100 {
		config.SetMaxItemsCount(i)
	}
}

func TestConfig_SetDataFileSize(t *testing.T) {
	config := NewConfig()
	defer config.Close()
	for i := 1; i < 1000 * 1000; i *= 100 {
		config.SetDataFileSize(i)
	}
}

func TestConfig_SetIndexFile(t *testing.T) {
	config := NewConfig()
	defer config.Close()
	for i := 1; i < 1000 * 1000; i *= 100 {
		config.SetIndexFile(fmt.Sprintf("foobar_%d.index", i))
	}
}

func TestConfig_SetDataFile(t *testing.T) {
	config := NewConfig()
	defer config.Close()
	for i := 1; i < 1000 * 1000; i *= 100 {
		config.SetDataFile(fmt.Sprintf("foobar_%d.data", i))
	}
}

func TestConfig_SetHotItemsCount(t *testing.T) {
	config := NewConfig()
	defer config.Close()
	for i := 1; i < 1000 * 1000; i *= 100 {
		config.SetHotItemsCount(i)
	}
}

func TestConfig_SetHotDataSize(t *testing.T) {
	config := NewConfig()
	defer config.Close()
	for i := 1; i < 1000 * 1000; i *= 100 {
		config.SetHotDataSize(i)
	}
}

func TestConfig_SetDeHashtableSize(t *testing.T) {
	config := NewConfig()
	defer config.Close()
	for i := 1; i < 1000 * 1000; i *= 100 {
		config.SetDeHashtableSize(i)
	}
}

func TestConfig_SetSyncInterval(t *testing.T) {
	config := NewConfig()
	defer config.Close()
	for i := 1; i < 1000 * 1000; i *= 100 {
		config.SetSyncInterval(time.Second * time.Duration(i))
	}
}

func TestConfig_RemoveCache_anonymous(t *testing.T) {
	config := NewConfig()
	defer config.Close()
	config.RemoveCache()
}

func TestConfig_RemoveCache_real(t *testing.T) {
	config := NewConfig()
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

func TestConfig_OpenCache_anonymous(t *testing.T) {
	config := NewConfig()
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

func TestConfig_OpenCache_existing(t *testing.T) {
	config := NewConfig()
	defer config.Close()

	config.SetDataFile("foobar.data")
	config.SetIndexFile("foobar.index")
	defer config.RemoveCache()

	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	cache.Close()

	for i := 1; i < 10; i++ {
		cache, err := config.OpenCache(false)
		if err != nil {
			t.Fatal(err)
		}
		cache.Close()
	}
}
