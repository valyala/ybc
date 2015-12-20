// Development version of debug helpers

// +build debug

package ybc

import (
	"log"
	"path/filepath"
	"runtime"
	"sync"
)

/*******************************************************************************
 * debugGuard
 ******************************************************************************/

type debugGuard struct {
	isLive bool
}

func debugGuardFinalizer(dg *debugGuard) {
	log.Fatalf("Unclosed object=%p at destruction time. Forgot calling Close() on the object?", dg)
}

func (dg *debugGuard) Init() {
	dg.init()
	runtime.SetFinalizer(dg, debugGuardFinalizer)
}

func (dg *debugGuard) CheckLive() {
	if !dg.isLive {
		panic("The object cannot be used, because it is already closed.")
	}
}

func (dg *debugGuard) Close() {
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
	var err error
	defer func() {
		if err != nil {
			releaseFile(cg.dataFile)
		}
	}()

	acquireFile(cg.dataFile)
	err = errPanic
	acquireFile(cg.indexFile)
	err = nil
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
		panic("Cannot open already opened index or data file")
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
		panic("Impossible happened: the given file is already closed!")
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
 * misc functions
 ******************************************************************************/

func checkNonNegative(n int) {
	if n < 0 {
		panic("The number cannot be negative")
	}
}
