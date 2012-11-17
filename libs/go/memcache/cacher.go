package memcache

import (
	"time"
)

// Client and DistributedClient implement this interface.
type Ccacher interface {
	Cget(item *Citem) error
	Cset(item *Citem) error
	CsetNowait(item *Citem)
}

// Client, DistributedClient and CachingClient implement this interface.
type Memcacher interface {
	Start()
	Stop()
	Get(item *Item) error
	GetMulti(items []Item) error
	GetDe(item *Item, graceDuration time.Duration) error
	Set(item *Item) error
	SetNowait(item *Item)
	Delete(key []byte) error
	DeleteNowait(key []byte)
	FlushAll() error
	FlushAllNowait()
	FlushAllDelayed(expiration time.Duration) error
	FlushAllDelayedNowait(expiration time.Duration)
}

// Client and DistributedClient implement this interface.
type Cacher interface {
	Memcacher
	Ccacher
}
