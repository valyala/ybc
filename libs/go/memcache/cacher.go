package memcache

import (
	"time"
)

// Client and DistributedClient implement this interface
type Cacher interface {
	Start()
	Stop()
	Get(item *Item) error
	GetMulti(keys [][]byte) (items []Item, err error)
	GetDe(item *Item, graceDuration time.Duration) error
	Cget(item *Citem) error
	Set(item *Item) error
	SetNowait(item *Item)
	Cset(item *Citem) error
	CsetNowait(item *Citem)
	Delete(key []byte) error
	DeleteNowait(key []byte)
	FlushAll() error
	FlushAllNowait()
	FlushAllDelayed(expiration time.Duration) error
	FlushAllDelayedNowait(expiration time.Duration)
}
