package ybc

import (
	"sync"
	"testing"
)

type benchmark_InitFunc func(cache Cacher)
type benchmark_IterationFunc func(cache Cacher)

func runBenchmark(initFunc benchmark_InitFunc, iterationFunc benchmark_IterationFunc, workersCount int, b *testing.B) {
	b.StopTimer()

	config := newConfig()
	cache, err := config.OpenCache(true)
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	initFunc(cache)

	wg := sync.WaitGroup{}
	defer wg.Wait()

	batchSize := 1000
	loopsCount := b.N
	ch := make(chan int, b.N/batchSize+1)
	for {
		if batchSize > loopsCount {
			ch <- loopsCount
			break
		}
		loopsCount -= batchSize
		ch <- batchSize
	}
	defer close(ch)

	benchLoop := func() {
		defer wg.Done()
		for loopsCount := range ch {
			for i := 0; i < loopsCount; i++ {
				iterationFunc(cache)
			}
		}
	}

	b.StartTimer()
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go benchLoop()
	}
}

func BenchmarkCache_GetHit_1(b *testing.B) {
	benchmarkCache_GetHit(b, 1)
}

func BenchmarkCache_GetHit_2(b *testing.B) {
	benchmarkCache_GetHit(b, 2)
}

func BenchmarkCache_GetHit_4(b *testing.B) {
	benchmarkCache_GetHit(b, 4)
}

func BenchmarkCache_GetHit_8(b *testing.B) {
	benchmarkCache_GetHit(b, 8)
}

func BenchmarkCache_GetHit_16(b *testing.B) {
	benchmarkCache_GetHit(b, 16)
}

func benchmarkCache_GetHit(b *testing.B, workersCount int) {
	key := []byte("12345")
	value := []byte("value")

	initFunc := func(cache Cacher) {
		if err := cache.Set(key, value, MaxTtl); err != nil {
			b.Fatalf("Cannot set item with key=[%s], value=[%s]: [%s]", key, value, err)
		}
	}

	iterationFunc := func(cache Cacher) {
		if _, err := cache.Get(key); err != nil {
			b.Fatalf("Cannot find item with key=[%s]: [%s]", key, err)
		}
	}

	runBenchmark(initFunc, iterationFunc, workersCount, b)
}

func BenchmarkCache_GetMiss_1(b *testing.B) {
	benchmarkCache_GetMiss(b, 1)
}

func BenchmarkCache_GetMiss_2(b *testing.B) {
	benchmarkCache_GetMiss(b, 2)
}

func BenchmarkCache_GetMiss_4(b *testing.B) {
	benchmarkCache_GetMiss(b, 4)
}

func BenchmarkCache_GetMiss_8(b *testing.B) {
	benchmarkCache_GetMiss(b, 8)
}

func BenchmarkCache_GetMiss_16(b *testing.B) {
	benchmarkCache_GetMiss(b, 16)
}

func benchmarkCache_GetMiss(b *testing.B, workersCount int) {
	key := []byte("12345")

	initFunc := func(cache Cacher) {}

	iterationFunc := func(cache Cacher) {
		if _, err := cache.Get(key); err != ErrCacheMiss {
			b.Fatalf("Unexpected item found for key=[%s]", key)
		}
	}

	runBenchmark(initFunc, iterationFunc, workersCount, b)
}

func BenchmarkCache_Set_1(b *testing.B) {
	benchmarkCache_Set(b, 1)
}

func BenchmarkCache_Set_2(b *testing.B) {
	benchmarkCache_Set(b, 2)
}

func BenchmarkCache_Set_4(b *testing.B) {
	benchmarkCache_Set(b, 4)
}

func BenchmarkCache_Set_8(b *testing.B) {
	benchmarkCache_Set(b, 8)
}

func BenchmarkCache_Set_16(b *testing.B) {
	benchmarkCache_Set(b, 16)
}

func benchmarkCache_Set(b *testing.B, workersCount int) {
	key := []byte("12345")
	value := []byte("value")

	initFunc := func(cache Cacher) {}

	iterationFunc := func(cache Cacher) {
		if err := cache.Set(key, value, MaxTtl); err != nil {
			b.Fatalf("Error in Cache.Set(key=[%s], value=[%s])=[%s]", key, value, err)
		}
	}

	runBenchmark(initFunc, iterationFunc, workersCount, b)
}
