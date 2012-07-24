// Development version of debugGuard implementation

// +build !release

package ybc

import (
	"runtime"
	"time"
)

type debugGuard struct {
	isClosed bool
}

func debugGuardFinalizer(dg *debugGuard) {
	panic("Unclosed object at destruction time. Forgot calling Close() on the object?")
}

func (dg *debugGuard) Init() {
	dg.isClosed = false
	runtime.SetFinalizer(dg, debugGuardFinalizer)
}

func (dg *debugGuard) CheckLive() {
	if dg.isClosed {
		panic("The object cannot be used, because it is already closed.")
	}
}

func (dg *debugGuard) SetClosed() {
	dg.isClosed = true
	runtime.SetFinalizer(dg, nil)
}

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
