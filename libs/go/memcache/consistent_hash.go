package memcache

import (
	"encoding/binary"
	"hash/fnv"
	"log"
)

type chItem struct {
	keyUint uint32
	value   interface{}
	next    *chItem
}

type consistentHash struct {
	ReplicasCount int
	BucketsCount  int

	buckets []*chItem
}

func (h *consistentHash) Init() {
	h.buckets = make([]*chItem, h.BucketsCount)
}

func (h *consistentHash) bucketIdx(key []byte, i int) (keyUint uint32, idx int) {
	fnvH := fnv.New32()
	fnvH.Write(key)
	if err := binary.Write(fnvH, binary.LittleEndian, uint32(i)); err != nil {
		log.Panicf("binary.Write() failed: [%s]", err)
	}
	keyUint = fnvH.Sum32()
	idx = int(keyUint % uint32(h.BucketsCount))
	return
}

func (h *consistentHash) getItemPtr(key []byte, i int) (itemPtr **chItem, keyUint uint32, idx int) {
	keyUint, idx = h.bucketIdx(key, i)
	itemPtr = &h.buckets[idx]
	item := *itemPtr
	for item != nil && item.keyUint < keyUint {
		itemPtr = &item.next
		item = *itemPtr
	}
	return
}

func (h *consistentHash) Add(key []byte, value interface{}) {
	for i := 0; i < h.ReplicasCount; i++ {
		itemPtr, keyUint, _ := h.getItemPtr(key, i)
		item := *itemPtr
		if item != nil && item.keyUint == keyUint {
			item.value = value
		} else {
			*itemPtr = &chItem{
				keyUint: keyUint,
				value:   value,
				next:    item,
			}
		}
	}
}

func (h *consistentHash) Delete(key []byte) {
	for i := 0; i < h.ReplicasCount; i++ {
		keyUint, idx := h.bucketIdx(key, i)
		itemPtr := &h.buckets[idx]
		item := *itemPtr
		for item != nil {
			if item.keyUint == keyUint {
				break
			}
			itemPtr = &item.next
			item = *itemPtr
		}
		if item != nil {
			*itemPtr = item.next
		}
	}
}

func (h *consistentHash) Get(key []byte) interface{} {
	itemPtr, _, idx := h.getItemPtr(key, 0)
	item := *itemPtr
	if item != nil {
		return item.value
	}

	for i := 0; i < h.BucketsCount; i++ {
		idx++
		if idx >= h.BucketsCount {
			idx = 0
		}
		item = h.buckets[idx]
		if item != nil {
			return item.value
		}
	}
	panic("The consistentHash is empty")
}
