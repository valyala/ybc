package memcache

import (
	"fmt"
	"testing"
)

func Test_consistentHash_Init(t *testing.T) {
	var h consistentHash
	for replicasCount := 1; replicasCount <= 1000; replicasCount *= 10 {
		for bucketsCount := 1; bucketsCount <= 1000; bucketsCount *= 10 {
			h.ReplicasCount = replicasCount
			h.BucketsCount = bucketsCount
			h.Init()
		}
	}
}

func checkConsistentHashGet(h *consistentHash, loopsCount int, t *testing.T) {
	for i := 0; i < loopsCount; i++ {
		key := []byte(fmt.Sprintf("aaa_%d", i))
		h.Get(key)
	}
}

func Test_consistentHash_AddDeleteDuplicate(t *testing.T) {
	h := consistentHash{
		ReplicasCount: 100,
		BucketsCount:  1000,
	}
	h.Init()

	key := []byte("key")
	for i := 0; i < 200; i++ {
		h.Add(key, i)
	}
	for i := 0; i < 300; i++ {
		h.Delete(key)
	}
}

func Test_consistentHash_AddGetDelete(t *testing.T) {
	itemsCount := 100
	loopsCount := 1000

	h := consistentHash{
		ReplicasCount: 100,
		BucketsCount:  1000,
	}
	h.Init()

	for i := 0; i < itemsCount; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		h.Add(key, i)
		checkConsistentHashGet(&h, loopsCount, t)
	}

	for i := 0; i < itemsCount/2; i++ {
		checkConsistentHashGet(&h, loopsCount, t)
		key := []byte(fmt.Sprintf("key_%d", i))
		h.Delete(key)
	}

	for i := itemsCount / 2; i < itemsCount; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		h.Add(key, i)
		checkConsistentHashGet(&h, loopsCount, t)
	}

	for i := 0; i < itemsCount; i++ {
		checkConsistentHashGet(&h, loopsCount, t)
		key := []byte(fmt.Sprintf("key_%d", i))
		h.Delete(key)
	}
}
