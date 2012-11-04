package memcache

import (
	"testing"
	"time"
)

func TestClient_StartStop(t *testing.T) {
	c := &Client{
		Addr: "localhost:12221",
		ReconnectTimeout: time.Millisecond * time.Duration(10),
	}
	c.Start()
	c.Stop()
}

func TestClient_StartStop_Multi(t *testing.T) {
	c := &Client{
		Addr: "localhost:12221",
		ReconnectTimeout: time.Millisecond * time.Duration(10),
	}
	for i := 0; i < 3; i++ {
		c.Start()
		c.Stop()
	}
}
