package memcache

import (
	"bytes"
	"github.com/valyala/ybc/bindings/go/ybc"
	"testing"
	"time"
)

func newCachingClientServerCache(t *testing.T) (cc *CachingClient, s *Server, cache ybc.Cacher) {
	c, s, cache := newClientServerCache(t)
	c.Start()
	cc = &CachingClient{
		Client: c,
		Cache:  newCache(t),
	}
	return
}

func verifyItem(item *Item, value []byte, flags uint32, location string, t *testing.T) {
	if !bytes.Equal(item.Value, value) {
		t.Fatalf("Unexpected item.Value=[%s]. Expected [%s]. location=[%s]", item.Value, value, location)
	}
	if item.Flags != flags {
		t.Fatalf("Unexpected item.Flags=%d. Expected %d. location=[%s]", item.Flags, flags, location)
	}
}

func TestCachingClient_SetGetDelete(t *testing.T) {
	c, s, cache := newCachingClientServerCache(t)
	defer cache.Close()
	defer s.Stop()
	defer c.Cache.Close()
	defer c.Client.(Cacher).Stop()

	key := []byte("key")
	value := []byte("value")
	flags := uint32(23899)

	item := Item{
		Key:   key,
		Value: value,
		Flags: flags,
	}

	// Both local cache and server don't contain the item now.
	if err := c.Get(&item); err != ErrCacheMiss {
		t.Fatalf("Unexpected error returned from CachingClient.Get(): [%s]. Expected ErrCacheMiss", err)
	}

	// Server should contain the item after this call.
	if err := c.Set(&item); err != nil {
		t.Fatalf("Error in CachingClient.Set(): [%s]", err)
	}

	// Local cache should populate the item after this call.
	item.Value = nil
	item.Flags = 0
	if err := c.Get(&item); err != nil {
		t.Fatalf("Error in CachingClient.Get() when obtaining item from the server: [%s]", err)
	}
	verifyItem(&item, value, flags, "1", t)

	// The item should be returned from the local cache now
	// (with revalidation on the server).
	for i := 0; i < 10; i++ {
		item.Value = nil
		item.Flags = 0
		if err := c.Get(&item); err != nil {
			t.Fatalf("Error in CachingClient.Get() when revalidating locally cached item: [%s]", err)
		}
		verifyItem(&item, value, flags, "2", t)
	}

	// Store the item, which may be locally cached for extended period of time
	key = []byte("new_key")
	value = []byte("new_value")
	item.Key = key
	item.Value = value
	validateTtl := time.Millisecond * 100
	if err := c.SetWithValidateTtl(&item, validateTtl); err != nil {
		t.Fatalf("Error in CachingClient.SetWithValidateTtl(): [%s]", err)
	}

	// Local cache should populate the item after this call.
	item.Value = nil
	item.Flags = 0
	if err := c.Get(&item); err != nil {
		t.Fatalf("Error in CachingClient.Get() when obtaining item from the server: [%s]", err)
	}
	verifyItem(&item, value, flags, "3", t)

	// The item should be returned from the local cache now
	// (without revalidation on the server).
	for i := 0; i < 10; i++ {
		item.Value = nil
		item.Flags = 0
		if err := c.Get(&item); err != nil {
			t.Fatalf("Error in CachingClient.Get() when obtaining locally cached item without validation: [%s]", err)
		}
		verifyItem(&item, value, flags, "4", t)
	}

	// Sleep for short period of time and make sure item revalidation works
	time.Sleep(validateTtl + time.Millisecond*10)

	// The item should be returned from the local cache now
	// (with revalidation on the server during the first iteration).
	for i := 0; i < 10; i++ {
		item.Value = nil
		item.Flags = 0
		if err := c.Get(&item); err != nil {
			t.Fatalf("Error in CachingClient.Get() when obtaining locally cached item on iteration %d (iteration 0 incurs item revalidation): [%s]", i, err)
		}
		verifyItem(&item, value, flags, "5", t)
	}

	// Delete the item and make sure it is really deleted.
	if err := c.Delete(item.Key); err != nil {
		t.Fatalf("Error in CachingClient.Delete(): [%s]", err)
	}

	for i := 0; i < 10; i++ {
		item.Value = nil
		item.Flags = 0
		if err := c.Get(&item); err != ErrCacheMiss {
			t.Fatalf("Unexpected error in CachingClient.Get() when trying to obtain deleted item: [%s]", err)
		}
	}

	if err := c.Delete(item.Key); err != ErrCacheMiss {
		t.Fatalf("Unexpected error in CachingClient.Delete() on already deleted item: [%s]", err)
	}
}
