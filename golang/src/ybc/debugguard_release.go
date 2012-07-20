// Release version of debugGuard implementation

// +build release

package ybc

type debugGuard struct {}

func (dg *debugGuard) Init() {}

func (dg *debugGuard) CheckLive() {}

func (dg *debugGuard) SetClosed() {}
