package memcache

import (
	"bytes"
	"fmt"
	"github.com/valyala/ybc/bindings/go/ybc"
	"testing"
	"time"
)

const (
	testAddr = "localhost:12345"
)

func newCache(t *testing.T) *ybc.Cache {
	config := ybc.NewConfig(1000*1000, 10*1000*1000)
	defer config.Close()

	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func newServerCache(t *testing.T) (s *Server, cache *ybc.Cache) {
	cache = newCache(t)
	s = &Server{
		Cache:      cache,
		ListenAddr: testAddr,
	}
	return
}

func TestServer_StartStop(t *testing.T) {
	s, cache := newServerCache(t)
	defer cache.Close()
	s.Start()
	s.Stop()
}

func TestServer_StartStop_Multi(t *testing.T) {
	s, cache := newServerCache(t)
	defer cache.Close()
	for i := 0; i < 3; i++ {
		s.Start()
		s.Stop()
	}
}

func TestServer_Serve(t *testing.T) {
	s, cache := newServerCache(t)
	defer cache.Close()
	go func() {
		time.Sleep(time.Millisecond * time.Duration(100))
		s.Stop()
	}()
	s.Serve()
}

func TestServer_Wait(t *testing.T) {
	s, cache := newServerCache(t)
	defer cache.Close()
	go func() {
		time.Sleep(time.Millisecond * time.Duration(100))
		s.Stop()
	}()
	s.Start()
	s.Wait()
}

func newClientServerCache(t *testing.T) (c *Client, s *Server, cache *ybc.Cache) {
	c = &Client{
		ConnectAddr:      testAddr,
		ReconnectTimeout: time.Millisecond * time.Duration(100),
	}
	s, cache = newServerCache(t)
	s.Start()
	return
}

func TestClient_StartStop(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()
	c.Start()
	c.Stop()
}

func TestClient_StartStop_Multi(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()
	for i := 0; i < 3; i++ {
		c.Start()
		c.Stop()
	}
}

func TestClient_GetSet(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	key := "key"
	value := []byte("value")

	item, err := c.Get(key)
	if err != ErrCacheMiss {
		t.Fatalf("Unexpected err=[%s] for client.Get(%s)", err, key)
	}

        item = &Item{
                Key: key,
                Value: value,
        }
        err = c.Set(item)
        if err != nil {
                t.Fatalf("error in client.Set(): [%s]", err)
        }
        item, err = c.Get(key)
        if err != nil {
                t.Fatalf("cannot obtain value for key=[%s] from memcache: [%s]", key, err)
        }
        if item.Key != key {
                t.Fatalf("Invalid key=[%s] returned. Expected [%s]", item.Key, key)
        }
        if !bytes.Equal(item.Value, value) {
                t.Fatalf("invalid value=[%s] returned. Expected [%s]", item.Value, value)
        }
}

func checkItems(c *Client, orig_items map[string]*Item, t *testing.T) {
	keys := make([]string, 0, len(orig_items))
	for key := range orig_items {
		keys = append(keys, key)
	}

	items, err := c.GetMulti(keys)
	if err != nil {
		t.Fatalf("Error in client.GetMulti(): [%s]", err)
	}
	for key, item := range items {
		orig_item := orig_items[key]
		if !bytes.Equal(item.Value, orig_item.Value) {
			t.Fatalf("Values mismatch for key=[%s]. Returned=[%s], expected=[%s]", key, item.Value, orig_item.Value)
		}
	}
}

func TestClient_GetMulti(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	itemsCount := 100
	items := make(map[string]*Item, itemsCount)
	for i := 0; i < itemsCount; i++ {
		item := &Item{
			Key: fmt.Sprintf("key_%d", i),
			Value: []byte(fmt.Sprintf("value_%d", i)),
		}
		err := c.Set(item)
		if err != nil {
			t.Fatalf("error in client.Set(): [%s]", err)
		}
		items[item.Key] = item
	}

	checkItems(c, items, t)
}

func TestClient_SetNowait(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	itemsCount := 100
	items := make(map[string]*Item, itemsCount)
	for i := 0; i < itemsCount; i++ {
		item := &Item{
			Key: fmt.Sprintf("key_%d", i),
			Value: []byte(fmt.Sprintf("value_%d", i)),
		}
		c.SetNowait(item)
		items[item.Key] = item
	}

	checkItems(c, items, t)
}
