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
// It can save network bandidth between the client and memcache servers.
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
	ok = false
	if err := binaryRead(it, &etag, "etag"); err != nil {
		return
	}
	if err := binaryRead(it, &validateTtl, "validateTtl"); err != nil {
		return
	}
	if err := binaryRead(it, &flags, "flags"); err != nil {
		return
	}
	if err := binaryRead(it, &validateExpiration64, "validateExpiration"); err != nil {
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

func getAndCacheRemoteItem(client Ccacher, cache ybc.Cacher, item *Item) error {
	citem := Citem{
		Item: Item{
			Key: item.Key,
		},
		Etag: deniedEtag,
	}
	if err := client.Cget(&citem); err != nil {
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

func revalidateAndSetItemValue(client Ccacher, cache ybc.Cacher, it *ybc.Item, item *Item, etag uint64, validateTtl uint32) error {
	citem := Citem{
		Item: Item{
			Key: item.Key,
		},
		Etag: etag,
	}
	if err := client.Cget(&citem); err != nil {
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
	citem := Citem{
		Item:        *item,
		Etag:        getEtag(),
		ValidateTtl: uint32(validateTtl / time.Millisecond),
	}
	if err := c.Client.Cset(&citem); err != nil {
		return err
	}
	cacheCitem(c.Cache, &citem)
	return nil
}

// See Client.Set()
func (c *CachingClient) Set(item *Item) error {
	return c.SetWithValidateTtl(item, 0)
}

// See Client.Delete()
func (c *CachingClient) Delete(key []byte) error {
	c.Cache.Delete(key)
	return c.Client.Delete(key)
}
