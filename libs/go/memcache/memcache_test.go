package memcache

import (
	"github.com/valyala/ybc/bindings/go/ybc"
	"testing"
	"time"
)

const (
	testAddr = "localhost:12345"
)

func newCache(t *testing.T) *ybc.Cache {
	config := ybc.NewConfig(1000*1000, 10*1000*1000)
	defer config.Close()

	cache, err := config.OpenCache(true)
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func newServer(t *testing.T) *Server {
	return &Server{
		Cache:      newCache(t),
		ListenAddr: testAddr,
	}
}

func TestServer_StartStop(t *testing.T) {
	s := newServer(t)
	defer s.Cache.Close()
	s.Start()
	s.Stop()
}

func TestServer_StartStop_Multi(t *testing.T) {
	s := newServer(t)
	defer s.Cache.Close()
	for i := 0; i < 3; i++ {
		s.Start()
		s.Stop()
	}
}

func TestServer_Serve(t *testing.T) {
	s := newServer(t)
	defer s.Cache.Close()
	go func() {
		time.Sleep(time.Millisecond * time.Duration(100))
		s.Stop()
	}()
	s.Serve()
}

func TestServer_Wait(t *testing.T) {
	s := newServer(t)
	defer s.Cache.Close()
	go func() {
		time.Sleep(time.Millisecond * time.Duration(100))
		s.Stop()
	}()
	s.Start()
	s.Wait()
}

func newClientServer(t *testing.T) (c *Client, s *Server) {
	c = &Client{
		ConnectAddr:      testAddr,
		ReconnectTimeout: time.Millisecond * time.Duration(100),
	}
	s = newServer(t)
	s.Start()
	return
}

func TestClient_StartStop(t *testing.T) {
	c, s := newClientServer(t)
	defer s.Cache.Close()
	defer s.Stop()
	c.Start()
	c.Stop()
}

func TestClient_StartStop_Multi(t *testing.T) {
	c, s := newClientServer(t)
	defer s.Cache.Close()
	defer s.Stop()
	for i := 0; i < 3; i++ {
		c.Start()
		c.Stop()
	}
}
