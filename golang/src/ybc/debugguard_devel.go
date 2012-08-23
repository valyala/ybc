// Development version of debug helpers

// +build !release

package ybc

import (
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

/*******************************************************************************
 * debugGuard
 ******************************************************************************/

type debugGuard struct {
	isLive  bool
	noClose bool
}

func debugGuardFinalizer(dg *debugGuard) {
	panic("Unclosed object at destruction time. Forgot calling Close() on the object?")
}

func (dg *debugGuard) Init() {
	dg.init()
	runtime.SetFinalizer(dg, debugGuardFinalizer)
}

func (dg *debugGuard) InitNoClose() {
	dg.init()
	dg.noClose = true
}

func (dg *debugGuard) CheckLive() {
	if !dg.isLive {
		panic("The object cannot be used, because it is already closed.")
	}
}

func (dg *debugGuard) Close() {
	if dg.noClose {
		panic("The object cannot be closed!")
	}
	dg.CheckLive()
	dg.isLive = false
	runtime.SetFinalizer(dg, nil)
}

func (dg *debugGuard) init() {
	if dg.isLive {
		panic("Cannot initialize live object. Forgot calling Close() before Init()?")
	}
	dg.isLive = true
}

/*******************************************************************************
 * cacheGuard
 ******************************************************************************/

var (
	acquiredFilesMutex sync.Mutex
	acquiredFiles      = make(map[string]bool)
)

type cacheGuard struct {
	dataFile  *string
	indexFile *string
}

func (cg *cacheGuard) SetDataFile(dataFile string) {
	cg.dataFile = &dataFile
}

func (cg *cacheGuard) SetIndexFile(indexFile string) {
	cg.indexFile = &indexFile
}

func (cg *cacheGuard) Acquire() {
	acquireFile(cg.dataFile)
	acquireFile(cg.indexFile)
}

func (cg *cacheGuard) Release() {
	releaseFile(cg.dataFile)
	releaseFile(cg.indexFile)
}

func acquireFile(filename *string) {
	if filename == nil {
		return
	}

	acquiredFilesMutex.Lock()
	defer acquiredFilesMutex.Unlock()

	absFilename := absFile(filename)
	if acquiredFiles[absFilename] {
		panic("Cannot open already opened index or data file for the cache!")
	}
	acquiredFiles[absFilename] = true
}

func releaseFile(filename *string) {
	if filename == nil {
		return
	}

	acquiredFilesMutex.Lock()
	defer acquiredFilesMutex.Unlock()

	absFilename := absFile(filename)
	if !acquiredFiles[absFilename] {
		panic("Impossible happened: the given cache file is already closed!")
	}
	delete(acquiredFiles, absFilename)
}

func absFile(filename *string) string {
	absFilename, err := filepath.Abs(*filename)
	if err != nil {
		panic(err)
	}
	return absFilename
}

/*******************************************************************************
 * clusterCacheGuard
 ******************************************************************************/

type clusterCacheGuard []*cacheGuard

func debugAcquireClusterCache(configs []*Config) (ccg clusterCacheGuard) {
	defer func() {
		if r := recover(); r != nil {
			debugReleaseClusterCache(ccg)
			panic(r)
		}
	}()

	for _, c := range configs {
		c.cg.Acquire()
		ccg = append(ccg, &c.cg)
	}
	return
}

func debugReleaseClusterCache(ccg clusterCacheGuard) {
	for _, cg := range ccg {
		cg.Release()
	}
}

/*******************************************************************************
 * misc functions
 ******************************************************************************/

func checkNonNegative(n int) {
	if n < 0 {
		panic("The number cannot be negative")
	}
}

func checkNonNegativeDuration(t time.Duration) {
	if t < 0 {
		panic("The duration cannot be negative")
	}
}
