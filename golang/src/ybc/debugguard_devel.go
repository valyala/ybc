// Development version of debugGuard implementation

// +build !release

package ybc

import (
	"log"
	"runtime"
	"runtime/debug"
)

type debugGuard struct {
	isClosed bool
}

func debugGuardFinalizer(dg *debugGuard) {
	if !dg.isClosed {
		log.Fatalf("Unclosed object %p at destruction time. Forgot calling Close() on the object?", dg)
	}
}

func (dg *debugGuard) Init() {
	runtime.SetFinalizer(dg, debugGuardFinalizer)
}

func (dg *debugGuard) CheckLive() {
	if dg.isClosed {
		log.Fatalf("The object %p cannot be used, because it is already closed. Stack trace:\n%s", dg, debug.Stack())
	}
}

func (dg *debugGuard) SetClosed() {
	dg.isClosed = true
}
