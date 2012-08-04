// Development version of debugGuard implementation

// +build !release

package ybc

import (
	"runtime"
	"time"
)

type debugGuard struct {
	isLive bool
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

func (dg *debugGuard) SetClosed() {
	if dg.noClose {
		panic("The object cannot be closed!")
	}
	dg.isLive = false
	runtime.SetFinalizer(dg, nil)
}

func (dg *debugGuard) init() {
	if dg.isLive {
		panic("Cannot initialize live object. Forgot calling Close() before Init()?")
	}
	dg.isLive = true
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
