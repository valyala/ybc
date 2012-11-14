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
	config := ybc.Config{
		MaxItemsCount: 1000 * 1000,
		DataFileSize:  10 * 1000 * 1000,
	}

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
		ConnectionsCount: 1, // tests require single connection!
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

	key := []byte("key")
	value := []byte("value")
	flags := uint32(12345)

	item := Item{
		Key: key,
	}
	if err := c.Get(&item); err != ErrCacheMiss {
		t.Fatalf("Unexpected err=[%s] for client.Get(%s)", err, key)
	}

	item.Value = value
	item.Flags = flags
	if err := c.Set(&item); err != nil {
		t.Fatalf("error in client.Set(): [%s]", err)
	}
	item.Value = nil
	item.Flags = 0
	if err := c.Get(&item); err != nil {
		t.Fatalf("cannot obtain value for key=[%s] from memcache: [%s]", key, err)
	}
	if !bytes.Equal(item.Value, value) {
		t.Fatalf("invalid value=[%s] returned. Expected [%s]", item.Value, value)
	}
	if item.Flags != flags {
		t.Fatalf("invalid flags=[%d] returned. Expected [%d]", item.Flags, flags)
	}
}

func TestClient_GetDe(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	item := Item{
		Key: []byte("key"),
	}
	grace := 100 * time.Millisecond
	for i := 0; i < 3; i++ {
		if err := c.GetDe(&item, grace); err != ErrCacheMiss {
			t.Fatalf("Unexpected err=[%s] for client.GetDe(%s, %d): [%s]", item.Key, grace, err)
		}
	}

	item.Value = []byte("value")
	if err := c.Set(&item); err != nil {
		t.Fatalf("Cannot set value=[%s] for key=[%s]: [%s]", item.Value, item.Key, err)
	}
	oldValue := item.Value
	item.Value = nil
	if err := c.GetDe(&item, grace); err != nil {
		t.Fatalf("Cannot obtain value fro key=[%s]: [%s]", item.Key, err)
	}
	if !bytes.Equal(oldValue, item.Value) {
		t.Fatalf("Unexpected value obtained: [%s]. Expected [%s]", item.Value, oldValue)
	}
}

func TestClient_CGetCSet(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	key := []byte("key")
	value := []byte("value")
	expiration := time.Hour * 123343

	etag := uint64(1234567890)
	validateTtl := time.Millisecond * 98765432
	item := Citem{
		Key:         key,
		Value:       value,
		Etag:        etag,
		Expiration:  expiration,
		ValidateTtl: validateTtl,
	}

	if err := c.CGet(&item); err != ErrCacheMiss {
		t.Fatalf("Unexpected error returned from Client.CGet(): [%s]. Expected ErrCacheMiss", err)
	}

	if err := c.CSet(&item); err != nil {
		t.Fatalf("Error in Client.CSet(): [%s]", err)
	}

	if err := c.CGet(&item); err != ErrNotModified {
		t.Fatalf("Unexpected error returned from Client.CGet(): [%s]. Expected ErrNotModified", err)
	}

	item.Value = nil
	item.Etag = 3234898
	item.Expiration = expiration + 10000*time.Second
	if err := c.CGet(&item); err != nil {
		t.Fatalf("Unexpected error returned from Client.CGet(): [%s]", err)
	}
	if item.Etag != etag {
		t.Fatalf("Unexpected etag=[%d] returned from Client.CGet(). Expected [%d]", item.Etag, etag)
	}
	if item.ValidateTtl != validateTtl {
		t.Fatalf("Unexpected validateTtl=[%d] returned from Client.CGet(). Expected [%d]", item.ValidateTtl, validateTtl)
	}
	if !bytes.Equal(item.Value, value) {
		t.Fatalf("Unexpected value=[%s] returned from Client.CGet(). Expected [%d]", item.Value, value)
	}
	if item.Expiration > expiration {
		t.Fatalf("Unexpected expiration=[%d] returned from Client.CGet(). Expected not more than [%d]", item.Expiration, expiration)
	}
}

func lookupItem(items []Item, key []byte) *Item {
	for i := 0; i < len(items); i++ {
		if bytes.Equal(items[i].Key, key) {
			return &items[i]
		}
	}
	return nil
}

func checkItems(c *Client, orig_items []Item, t *testing.T) {
	keys := make([][]byte, 0, len(orig_items))
	for _, item := range orig_items {
		keys = append(keys, item.Key)
	}

	items, err := c.GetMulti(keys)
	if err != nil {
		t.Fatalf("Error in client.GetMulti(): [%s]", err)
	}
	for _, item := range items {
		orig_item := lookupItem(orig_items, item.Key)
		if orig_item == nil {
			t.Fatalf("Cannot find original item with key=[%s]", item.Key)
		}
		if !bytes.Equal(item.Value, orig_item.Value) {
			t.Fatalf("Values mismatch for key=[%s]. Returned=[%s], expected=[%s]", item.Key, item.Value, orig_item.Value)
		}
	}
}

func checkCItems(c *Client, items []Citem, t *testing.T) {
	for i := 0; i < len(items); i++ {
		item := items[i]
		err := c.CGet(&item)
		if err == ErrCacheMiss {
			continue
		}
		if err != ErrNotModified {
			t.Fatalf("Unexpected error returned from Client.CGet(): [%s]. Expected ErrNotModified", err)
		}

		item.Etag++
		if err := c.CGet(&item); err != nil {
			t.Fatalf("Error when calling Client.CGet(): [%s]", err)
		}
		if item.Etag != items[i].Etag {
			t.Fatalf("Unexpected etag=%d returned. Expected %d", item.Etag, items[i].Etag)
		}
		if item.ValidateTtl != items[i].ValidateTtl {
			t.Fatalf("Unexpected validateTtl=%d returned. Expected %d", item.ValidateTtl, items[i].ValidateTtl)
		}
		if !bytes.Equal(item.Value, items[i].Value) {
			t.Fatalf("Unexpected value=[%s] returned. Expected [%s]", item.Value, items[i].Value)
		}
	}
}

func TestClient_GetMulti(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	itemsCount := 1000
	items := make([]Item, itemsCount)
	for i := 0; i < itemsCount; i++ {
		item := &items[i]
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		item.Value = []byte(fmt.Sprintf("value_%d", i))
		if err := c.Set(item); err != nil {
			t.Fatalf("error in client.Set(): [%s]", err)
		}
	}

	checkItems(c, items, t)
}

func TestClient_SetNowait(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	itemsCount := 1000
	items := make([]Item, itemsCount)
	for i := 0; i < itemsCount; i++ {
		item := &items[i]
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		item.Value = []byte(fmt.Sprintf("value_%d", i))
		c.SetNowait(item)
	}

	checkItems(c, items, t)
}

func TestClient_CSetNowait(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	itemsCount := 1000
	items := make([]Citem, itemsCount)
	for i := 0; i < itemsCount; i++ {
		item := &items[i]
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		item.Value = []byte(fmt.Sprintf("value_%d", i))
		item.Etag = uint64(i)
		item.ValidateTtl = time.Second * time.Duration(i)
		c.CSetNowait(item)
	}

	checkCItems(c, items, t)
}

func TestClient_Delete(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	itemsCount := 100
	var item Item
	for i := 0; i < itemsCount; i++ {
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		item.Value = []byte(fmt.Sprintf("value_%d", i))
		if err := c.Delete(item.Key); err != ErrCacheMiss {
			t.Fatalf("error when deleting non-existing item: [%s]", err)
		}
		if err := c.Set(&item); err != nil {
			t.Fatalf("error in client.Set(): [%s]", err)
		}
		if err := c.Delete(item.Key); err != nil {
			t.Fatalf("error when deleting existing item: [%s]", err)
		}
		if err := c.Delete(item.Key); err != ErrCacheMiss {
			t.Fatalf("error when deleting non-existing item: [%s]", err)
		}
	}
}

func TestClient_DeleteNowait(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	itemsCount := 100
	var item Item
	for i := 0; i < itemsCount; i++ {
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		item.Value = []byte(fmt.Sprintf("value_%d", i))
		if err := c.Set(&item); err != nil {
			t.Fatalf("error in client.Set(): [%s]", err)
		}
	}
	for i := 0; i < itemsCount; i++ {
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		c.DeleteNowait(item.Key)
	}
	for i := 0; i < itemsCount; i++ {
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		if err := c.Get(&item); err != ErrCacheMiss {
			t.Fatalf("error when obtaining deleted item: [%s]", err)
		}
	}
}

func TestClient_FlushAll(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	itemsCount := 100
	var item Item
	for i := 0; i < itemsCount; i++ {
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		item.Value = []byte(fmt.Sprintf("value_%d", i))
		if err := c.Set(&item); err != nil {
			t.Fatalf("error in client.Set(): [%s]", err)
		}
	}
	c.FlushAllNowait()
	c.FlushAll()
	for i := 0; i < itemsCount; i++ {
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		if err := c.Get(&item); err != ErrCacheMiss {
			t.Fatalf("error when obtaining deleted item: [%s]", err)
		}
	}
}

func TestClient_FlushAllDelayed(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	itemsCount := 100
	var item Item
	for i := 0; i < itemsCount; i++ {
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		item.Value = []byte(fmt.Sprintf("value_%d", i))
		if err := c.Set(&item); err != nil {
			t.Fatalf("error in client.Set(): [%s]", err)
		}
	}
	c.FlushAllDelayedNowait(time.Second)
	c.FlushAllDelayed(time.Second)
	foundItems := 0
	for i := 0; i < itemsCount; i++ {
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		err := c.Get(&item)
		if err == ErrCacheMiss {
			continue
		}
		if err != nil {
			t.Fatalf("error when obtaining item: [%s]", err)
		}
		foundItems++
	}
	if foundItems == 0 {
		t.Fatalf("It seems all the %d items are already delayed", itemsCount)
	}

	time.Sleep(time.Second * 2)
	for i := 0; i < itemsCount; i++ {
		item.Key = []byte(fmt.Sprintf("key_%d", i))
		if err := c.Get(&item); err != ErrCacheMiss {
			t.Fatalf("error when obtaining deleted item: [%s]", err)
		}
	}
}

func checkMalformedKey(c *Client, key string, t *testing.T) {
	item := Item{
		Key: []byte(key),
	}
	if err := c.Get(&item); err != ErrMalformedKey {
		t.Fatalf("Unexpected err=[%s] returned. Expected ErrMalformedKey", err)
	}
	if err := c.GetDe(&item, time.Second); err != ErrMalformedKey {
		t.Fatalf("Unexpected err=[%s] returned. Expected ErrMalformedKey", err)
	}
	if err := c.Set(&item); err != ErrMalformedKey {
		t.Fatalf("Unexpected err=[%s] returned. Expected ErrMalformedKey", err)
	}
	if err := c.Delete(item.Key); err != ErrMalformedKey {
		t.Fatalf("Unexpected err=[%s] returned. Expected ErrMalformedKey", err)
	}

	citem := Citem{
		Key: item.Key,
	}
	if err := c.CGet(&citem); err != ErrMalformedKey {
		t.Fatalf("Unexpected err=[%s] returned. Expected ErrMalformedKey", err)
	}
	if err := c.CSet(&citem); err != ErrMalformedKey {
		t.Fatalf("Unexpected err=[%s] returned. Expected ErrMalformedKey", err)
	}
}

func TestClient_MalformedKey(t *testing.T) {
	c, s, cache := newClientServerCache(t)
	defer cache.Close()
	defer s.Stop()

	c.Start()
	defer c.Stop()

	checkMalformedKey(c, "malformed key with spaces", t)
	checkMalformedKey(c, "malformed\nkey\nwith\nnewlines", t)
}
