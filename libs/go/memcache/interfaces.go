package memcache

import (
	"time"
)

// Client, DistributedClient and CachingClient implement this interface.
type Memcacher interface {
	Get(item *Item) error
	GetMulti(items []Item) error
	Set(item *Item) error
	SetNowait(item *Item)
	Delete(key []byte) error
	DeleteNowait(key []byte)
	Add(item *Item) error
	Cas(item *Item) error
	FlushAll() error
	FlushAllNowait()
	FlushAllDelayed(expiration time.Duration) error
	FlushAllDelayedNowait(expiration time.Duration)
}

// Client, DistributedClient and CachingClient implement this interface.
type MemcacherDe interface {
	Memcacher

	GetDe(item *Item, graceDuration time.Duration) error
}

// Client and DistributedClient implement this interface.
type Ccacher interface {
	MemcacherDe

	Cget(item *Item) error
	CgetDe(item *Item, graceDuration time.Duration) error
}

// Client and DistributedClient implement this interface.
type Cacher interface {
	Ccacher

	Start()
	Stop()
}
