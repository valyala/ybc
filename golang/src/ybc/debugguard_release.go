// Release version of debugGuard implementation

// +build release

package ybc

import (
	"time"
)

type debugGuard struct {}

func (dg *debugGuard) Init() {}

func (dg *debugGuard) CheckLive() {}

func (dg *debugGuard) SetClosed() {}

func checkNonNegative(n int) {}

func checkNonNegativeDuration(t time.Duration) {}
