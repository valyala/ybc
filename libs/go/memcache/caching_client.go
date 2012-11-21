package memcache

import (
	"encoding/binary"
	"github.com/valyala/ybc/bindings/go/ybc"
	"log"
	"math/rand"
	"time"
)

func init() {
	seed := time.Now().UnixNano()
	rand.Seed(seed)
}

// Memcache client with in-process data caching.
//
// It can save network bandwidth between the client and memcache servers.
//
// The client uses approach similar to HTTP cache validation with entity tags -
// see http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.11 .
//
// Usage:
//
//   cache := openCache()
//   defer cache.Close()
//
//   client.Start()
//   defer client.Stop()
//
//   c := memcache.CachingClient{
//       Client: client,
//       Cache:  cache,
//   }
//
//   item := memcache.Item{
//       Key:   []byte("key"),
//       Value: []byte("value"),
//   }
//   if err := c.Set(&item); err != nil {
//       log.Fatalf("Error in c.Set(): %s", err)
//   }
//   if err := c.Get(&item); err != nil {
//       log.Fatalf("Error in c.Get(): %s", err)
//   }
//
type CachingClient struct {
	// The underlying memcache client.
	//
	// The client must be initialized before passing it here.
	//
	// Currently Client and DistributedClient may be passed here.
	Client Ccacher

	// The underlying local cache.
	//
	// The cache should be initialized before passing it here.
	//
	// Currently ybc.Cache and ybc.Cluster may be passed here.
	Cache ybc.Cacher
}

const deniedEtag = 0

func getEtag() uint64 {
	for {
		if etag := uint64(rand.Int63()); etag != deniedEtag {
			return etag
		}
	}
	panic("unreachable")
}

func writeItemMetadata(cache ybc.Cacher, key []byte, size int, ttl time.Duration, etag uint64, validateTtl uint32, flags uint32) *ybc.SetTxn {
	validateExpiration64 := uint64(time.Now().Add(time.Duration(validateTtl) * time.Millisecond).UnixNano())

	size += binary.Size(&etag) + binary.Size(&validateTtl) + binary.Size(flags) + binary.Size(&validateExpiration64)
	txn, err := cache.NewSetTxn(key, size, ttl)
	if err != nil {
		log.Printf("Unexpected error in Cache.NewSetTxn(size=%d): [%s]", size, err)
		return nil
	}

	binaryWrite(txn, &etag, "etag")
	binaryWrite(txn, &validateTtl, "validateTtl")
	binaryWrite(txn, &flags, "flags")
	binaryWrite(txn, &validateExpiration64, "validateExpiration")

	return txn
}

func readItemMetadata(it *ybc.Item) (etag uint64, validateTtl uint32, flags uint32, validateExpiration time.Time, ok bool) {
	var validateExpiration64 uint64
	if !binaryRead(it, &etag, "etag") || !binaryRead(it, &validateTtl, "validateTtl") ||
		!binaryRead(it, &flags, "flags") || !binaryRead(it, &validateExpiration64, "validateExpiration") {
		ok = false
		return
	}
	validateExpiration = time.Unix(0, int64(validateExpiration64))
	ok = true
	return
}

func cacheItem(cache ybc.Cacher, item *Item, etag uint64, validateTtl uint32) {
	size := len(item.Value)
	txn := writeItemMetadata(cache, item.Key, size, item.Expiration, etag, validateTtl, item.Flags)
	if txn == nil {
		return
	}
	defer txn.Commit()

	n, err := txn.Write(item.Value)
	if err != nil {
		log.Fatalf("Unexpected error in SetTxn.Write(size=%d): [%s]", size, err)
	}
	if n != size {
		log.Fatalf("Unexpected number of bytes written in SetTxn.Write(size=%d): %d", size, n)
	}
}

func cacheCitem(cache ybc.Cacher, citem *Citem) {
	cacheItem(cache, &citem.Item, citem.Etag, citem.ValidateTtl)
}

func getAndCacheRemoteItemInternal(client Ccacher, cache ybc.Cacher, item *Item, getFunc func(*Citem) error) error {
	citem := Citem{
		Item: Item{
			Key: item.Key,
		},
		Etag: deniedEtag,
	}
	if err := getFunc(&citem); err != nil {
		if err == ErrNotModified {
			log.Fatalf("Unexpected result returned. Server said the item with deniedEtag=%d under the key=[%s] isn't modified", deniedEtag, citem.Key)
		}
		return err
	}
	cacheCitem(cache, &citem)
	item.Value = citem.Value
	item.Flags = citem.Flags
	return nil
}

func getAndCacheRemoteItem(client Ccacher, cache ybc.Cacher, item *Item) error {
	cGetFunc := func(citem *Citem) error {
		return client.Cget(citem)
	}
	return getAndCacheRemoteItemInternal(client, cache, item, cGetFunc)
}

func getDeAndCacheRemoteItem(client Ccacher, cache ybc.Cacher, item *Item, graceDuration time.Duration) error {
	cGetDeFunc := func(citem *Citem) error {
		return client.CgetDe(citem, graceDuration)
	}
	return getAndCacheRemoteItemInternal(client, cache, item, cGetDeFunc)
}

func setItemValue(it *ybc.Item, item *Item) error {
	size := it.Available()
	item.Value = make([]byte, size)
	n, err := it.Read(item.Value)
	if err != nil {
		log.Fatalf("Unexpected error in Item.Read(size=%d): [%s]", size, err)
	}
	if n != size {
		log.Fatalf("Unexpected number of bytes read in Item.Read(size=%d): %d", size, n)
	}
	return nil
}

func updateLocalItem(cache ybc.Cacher, it *ybc.Item, item *Item, etag uint64, validateTtl uint32) {
	size := it.Available()
	offset := it.Size() - size
	defer it.Seek(int64(offset), 0)

	txn := writeItemMetadata(cache, item.Key, size, it.Ttl(), etag, validateTtl, item.Flags)
	if txn == nil {
		return
	}
	defer txn.Commit()

	n, err := txn.ReadFrom(it)
	if err != nil {
		log.Fatalf("Unexpected error in SetTxn.ReadFrom(size=%d): [%s]", size, err)
	}
	if n != int64(size) {
		log.Fatalf("Unexpected number of bytes copied in SetTxn.ReadFrom(size=%d): %d", size, n)
	}

}

func revalidateAndSetItemValueInternal(client Ccacher, cache ybc.Cacher, it *ybc.Item, item *Item, etag uint64, validateTtl uint32, getFunc func(*Citem) error) error {
	citem := Citem{
		Item: Item{
			Key: item.Key,
		},
		Etag: etag,
	}
	if err := getFunc(&citem); err != nil {
		if err == ErrNotModified {
			if validateTtl > 0 {
				updateLocalItem(cache, it, item, etag, validateTtl)
			}
			return setItemValue(it, item)
		}
		if err == ErrCacheMiss {
			cache.Delete(item.Key)
		}
		return err
	}
	cacheCitem(cache, &citem)
	item.Value = citem.Value
	return nil
}

func revalidateAndSetItemValue(client Ccacher, cache ybc.Cacher, it *ybc.Item, item *Item, etag uint64, validateTtl uint32) error {
	cGetFunc := func(citem *Citem) error {
		return client.Cget(citem)
	}
	return revalidateAndSetItemValueInternal(client, cache, it, item, etag, validateTtl, cGetFunc)
}

func revalidateDeAndSetItemValue(client Ccacher, cache ybc.Cacher, it *ybc.Item, item *Item, etag uint64, validateTtl uint32, graceDuration time.Duration) error {
	cGetDeFunc := func(citem *Citem) error {
		return client.CgetDe(citem, graceDuration)
	}
	return revalidateAndSetItemValueInternal(client, cache, it, item, etag, validateTtl, cGetDeFunc)
}

// See Client.Get()
func (c *CachingClient) Get(item *Item) error {
	it, err := c.Cache.GetItem(item.Key)
	if err == ybc.ErrCacheMiss {
		return getAndCacheRemoteItem(c.Client, c.Cache, item)
	}
	if err != nil {
		log.Fatalf("Unexpected error returned from Cache.GetItem() for key=[%s]: [%s]", item.Key, err)
	}
	defer it.Close()

	etag, validateTtl, flags, validateExpiration, ok := readItemMetadata(it)
	if !ok {
		return getAndCacheRemoteItem(c.Client, c.Cache, item)
	}
	item.Flags = flags

	if time.Now().After(validateExpiration) {
		return revalidateAndSetItemValue(c.Client, c.Cache, it, item, etag, validateTtl)
	}
	return setItemValue(it, item)
}

// See Client.GetMulti()
func (c *CachingClient) GetMulti(items []Item) error {
	// TODO(valyala): implement Client.CgetMulti().
	itemsCount := len(items)
	for i := 0; i < itemsCount; i++ {
		if err := c.Get(&items[i]); err != nil {
			return err
		}
	}
	return nil
}

// See Client.GetDe()
func (c *CachingClient) GetDe(item *Item, graceDuration time.Duration) error {
	it, err := c.Cache.GetDeItem(item.Key, graceDuration)
	if err == ybc.ErrCacheMiss {
		return getDeAndCacheRemoteItem(c.Client, c.Cache, item, graceDuration)
	}
	if err != nil {
		log.Fatalf("Unexpected error returned from Cache.GetDeItem() for key=[%s]: [%s]", item.Key, err)
	}
	defer it.Close()

	etag, validateTtl, flags, validateExpiration, ok := readItemMetadata(it)
	if !ok {
		return getDeAndCacheRemoteItem(c.Client, c.Cache, item, graceDuration)
	}
	item.Flags = flags

	if time.Now().After(validateExpiration) {
		return revalidateDeAndSetItemValue(c.Client, c.Cache, it, item, etag, validateTtl, graceDuration)
	}
	return setItemValue(it, item)
}

// The same as CachingClient.Set(), but sets interval for periodic item
// revalidation on the server.
//
// This means that outdated, locally cached version of the item may be returned
// during validateTtl interval. Use CachingClient.Set() if you don't understand
// how this works.
//
// Setting validateTtl to 0 leads to item re-validation on every get() request.
// This is equivalent to CachingClient.Set() call. Even in this scenario network
// bandiwdth between the client and memcache servers is saved if the average
// item size exceeds ~100 bytes.
func (c *CachingClient) SetWithValidateTtl(item *Item, validateTtl time.Duration) error {
	c.Cache.Delete(item.Key)
	citem := Citem{
		Item:        *item,
		Etag:        getEtag(),
		ValidateTtl: uint32(validateTtl / time.Millisecond),
	}
	return c.Client.Cset(&citem)
}

// The same as CachingClient.SetWithValidateTtl(), but doesn't wait
// for completion of the operation.
func (c *CachingClient) SetWithValidateTtlNowait(item *Item, validateTtl time.Duration) {
	c.Cache.Delete(item.Key)
	citem := Citem{
		Item:        *item,
		Etag:        getEtag(),
		ValidateTtl: uint32(validateTtl / time.Millisecond),
	}
	c.Client.CsetNowait(&citem)
}

// See Client.Set()
func (c *CachingClient) Set(item *Item) error {
	return c.SetWithValidateTtl(item, 0)
}

// See Client.SetNowait()
func (c *CachingClient) SetNowait(item *Item) {
	c.SetWithValidateTtlNowait(item, 0)
}

// See Client.Delete()
func (c *CachingClient) Delete(key []byte) error {
	c.Cache.Delete(key)
	return c.Client.Delete(key)
}

// See Client.DeleteNowait()
func (c *CachingClient) DeleteNowait(key []byte) {
	c.Cache.Delete(key)
	c.Client.DeleteNowait(key)
}

// See Client.FlushAll()
func (c *CachingClient) FlushAll() error {
	c.Cache.Clear()
	return c.Client.FlushAll()
}

// See Client.FlushAllNowait()
func (c *CachingClient) FlushAllNowait() {
	c.Cache.Clear()
	c.Client.FlushAllNowait()
}

// See Client.FlushAllDelayed()
func (c *CachingClient) FlushAllDelayed(expiration time.Duration) error {
	time.AfterFunc(expiration, cacheClearFunc(c.Cache))
	return c.Client.FlushAllDelayed(expiration)
}

// See Client.FlushAllDelayedNowait()
func (c *CachingClient) FlushAllDelayedNowait(expiration time.Duration) {
	time.AfterFunc(expiration, cacheClearFunc(c.Cache))
	c.Client.FlushAllDelayedNowait(expiration)
}
