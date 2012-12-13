package memcache

import (
	"encoding/binary"
	"github.com/valyala/ybc/bindings/go/ybc"
	"log"
	"time"
)

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

const metadataSize = casidSize + flagsSize + validateTtlSize + validateExpirationSize

func writeItemMetadata(cache ybc.Cacher, key []byte, size int, casid uint64, flags, validateTtl uint32) *ybc.SetTxn {
	validateExpiration64 := uint64(time.Now().Add(time.Duration(validateTtl) * time.Millisecond).UnixNano())

	size += metadataSize
	txn, err := cache.NewSetTxn(key, size, ybc.MaxTtl)
	if err != nil {
		log.Printf("Unexpected error in Cache.NewSetTxn(size=%d): [%s]", size, err)
		return nil
	}

	var buf [metadataSize]byte
	binary.LittleEndian.PutUint64(buf[:casidSize], casid)
	binary.LittleEndian.PutUint32(buf[casidSize:], flags)
	binary.LittleEndian.PutUint32(buf[casidSize+flagsSize:], validateTtl)
	binary.LittleEndian.PutUint64(buf[casidSize+flagsSize+validateTtlSize:], validateExpiration64)
	n, err := txn.Write(buf[:])
	if err != nil {
		log.Fatalf("Error in SetTxn.Write(): [%s]", err)
	}
	if n != len(buf) {
		log.Fatalf("Unexpected result returned from SetTxn.Write(): %d. Expected %d", n, len(buf))
	}
	return txn
}

func readItemMetadata(it *ybc.Item) (casid uint64, flags uint32, validateExpiration time.Time, validateTtl uint32, ok bool) {
	var buf [metadataSize]byte
	n, err := it.Read(buf[:])
	if err != nil {
		log.Printf("Cannot read metadata for cached item: [%s]", err)
		return
	}
	if n != len(buf) {
		log.Printf("Unexpected result returned from ybc.Item.Read(): %d. Expected %d", n, len(buf))
		return
	}
	casid = binary.LittleEndian.Uint64(buf[:])
	flags = binary.LittleEndian.Uint32(buf[casidSize:])
	validateTtl = binary.LittleEndian.Uint32(buf[casidSize+flagsSize:])
	validateExpiration64 := binary.LittleEndian.Uint64(buf[casidSize+flagsSize+validateTtlSize:])
	validateExpiration = time.Unix(0, int64(validateExpiration64))
	ok = true
	return
}

func cacheItem(cache ybc.Cacher, item *Item) error {
	if len(item.Value) < validateTtlSize {
		log.Printf("Cannot read validateTtl from too short item.Value. Its' size is %d bytes, while expected size should be greater than %d", len(item.Value), validateTtlSize-1)
		return ErrCacheMiss
	}
	validateTtl := binary.LittleEndian.Uint32(item.Value)
	item.Value = item.Value[validateTtlSize:]
	txn := writeItemMetadata(cache, item.Key, len(item.Value), item.Casid, item.Flags, validateTtl)
	if txn == nil {
		return nil
	}
	// do not use defer txn.Commit() for performance reasons

	size := len(item.Value)
	n, err := txn.Write(item.Value)
	if err != nil {
		log.Fatalf("Unexpected error in SetTxn.Write(size=%d): [%s]", size, err)
	}
	if n != size {
		log.Fatalf("Unexpected number of bytes written in SetTxn.Write(size=%d): %d", size, n)
	}
	txn.Commit()
	return nil
}

func getAndCacheRemoteItem(client Ccacher, cache ybc.Cacher, item *Item) error {
	if err := client.Get(item); err != nil {
		return err
	}
	return cacheItem(cache, item)
}

func getDeAndCacheRemoteItem(client Ccacher, cache ybc.Cacher, item *Item, graceDuration time.Duration) error {
	if err := client.GetDe(item, graceDuration); err != nil {
		return err
	}
	return cacheItem(cache, item)
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

func updateLocalItem(cache ybc.Cacher, it *ybc.Item, key []byte, casid uint64, flags, validateTtl uint32) {
	size := it.Available()
	defer it.Seek(-int64(size), 2)

	txn := writeItemMetadata(cache, key, size, casid, flags, validateTtl)
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

func revalidateAndSetItemValueInternal(client Ccacher, cache ybc.Cacher, it *ybc.Item, item *Item, casid uint64, flags, validateTtl uint32, getFunc func() error) error {
	if err := getFunc(); err != nil {
		if err == ErrNotModified {
			if validateTtl > 0 {
				updateLocalItem(cache, it, item.Key, casid, flags, validateTtl)
			}
			return setItemValue(it, item)
		}
		if err == ErrCacheMiss {
			cache.Delete(item.Key)
		}
		return err
	}
	return cacheItem(cache, item)
}

func revalidateAndSetItemValue(client Ccacher, cache ybc.Cacher, it *ybc.Item, item *Item, casid uint64, flags, validateTtl uint32) error {
	getFunc := func() error {
		return client.Cget(item)
	}
	return revalidateAndSetItemValueInternal(client, cache, it, item, casid, flags, validateTtl, getFunc)
}

func revalidateDeAndSetItemValue(client Ccacher, cache ybc.Cacher, it *ybc.Item, item *Item, casid uint64, flags, validateTtl uint32, graceDuration time.Duration) error {
	getFunc := func() error {
		return client.CgetDe(item, graceDuration)
	}
	return revalidateAndSetItemValueInternal(client, cache, it, item, casid, flags, validateTtl, getFunc)
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
	// do not use defer it.Close() for performance reasons.

	casid, flags, validateExpiration, validateTtl, ok := readItemMetadata(it)
	if !ok {
		it.Close()
		return getAndCacheRemoteItem(c.Client, c.Cache, item)
	}
	item.Casid = casid
	item.Flags = flags

	if time.Now().After(validateExpiration) {
		err = revalidateAndSetItemValue(c.Client, c.Cache, it, item, casid, flags, validateTtl)
	} else {
		err = setItemValue(it, item)
	}
	it.Close()
	return err
}

// See Client.GetMulti()
func (c *CachingClient) GetMulti(items []Item) error {
	// TODO(valyala): optimize this
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
	// do not use defer it.Close() for performance reasons.

	casid, flags, validateExpiration, validateTtl, ok := readItemMetadata(it)
	if !ok {
		it.Close()
		return getDeAndCacheRemoteItem(c.Client, c.Cache, item, graceDuration)
	}
	item.Casid = casid
	item.Flags = flags

	if time.Now().After(validateExpiration) {
		err = revalidateDeAndSetItemValue(c.Client, c.Cache, it, item, casid, flags, validateTtl, graceDuration)
	} else {
		err = setItemValue(it, item)
	}
	it.Close()
	return err
}

func prependValidateTtl(value []byte, validateTtl time.Duration) []byte {
	validateTtl32 := uint32(validateTtl / time.Millisecond)
	size := len(value) + validateTtlSize
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[:], validateTtl32)
	copy(buf[validateTtlSize:], value)
	return buf
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
	item.Value = prependValidateTtl(item.Value, validateTtl)
	return c.Client.Set(item)
}

// The same as CachingClient.SetWithValidateTtl(), but doesn't wait
// for completion of the operation.
func (c *CachingClient) SetWithValidateTtlNowait(item *Item, validateTtl time.Duration) {
	c.Cache.Delete(item.Key)
	item.Value = prependValidateTtl(item.Value, validateTtl)
	c.Client.SetNowait(item)
}

// See Client.Set()
func (c *CachingClient) Set(item *Item) error {
	return c.SetWithValidateTtl(item, 0)
}

// The same as CachingClient.Add(), but sets interval for periodic item
// revalidation on the server.
//
// This means that outdated, locally cached version of the item may be returned
// during validateTtl interval. Use CachingClient.Add() if you don't unserstand
// how this works.
//
// Setting validateTtl to 0 leads to item re-validation on every get() request.
// This is equivalent ot CachingClient.Add() call. Even in this scenario network
// bandwidth between the client and memcache servers is saved if the average
// item size exceeds ~100 bytes.
func (c *CachingClient) AddWithValidateTtl(item *Item, validateTtl time.Duration) error {
	c.Cache.Delete(item.Key)
	item.Value = prependValidateTtl(item.Value, validateTtl)
	return c.Client.Add(item)
}

// See Client.Add()
func (c *CachingClient) Add(item *Item) error {
	return c.AddWithValidateTtl(item, 0)
}

// The same as CachingClient.Cas(), but sets interval for periodic item
// revalidation on the server.
//
// This means that outdated, locally cached version of the item may be returned
// during validateTtl interval. Use CachingClient.Cas() if you don't unserstand
// how this works.
//
// Setting validateTtl to 0 leads to item re-validation on every get() request.
// This is equivalent ot CachingClient.Cas() call. Even in this scenario network
// bandwidth between the client and memcache servers is saved if the average
// item size exceeds ~100 bytes.
func (c *CachingClient) CasWithValidateTtl(item *Item, validateTtl time.Duration) error {
	c.Cache.Delete(item.Key)
	item.Value = prependValidateTtl(item.Value, validateTtl)
	return c.Client.Cas(item)
}

// See Client.Cas()
func (c *CachingClient) Cas(item *Item) error {
	return c.CasWithValidateTtl(item, 0)
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
