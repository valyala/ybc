package memcache

import (
	"errors"
	"sync"
	"time"
)

const (
	consistentHashReplicasCount = 100
	consistentHashBucketsCount  = 1024
)

var (
	ErrNoServers = errors.New("memcache: no servers registered in DistributedClient")
)

// Memcache client, which can shard requests to multiple servers
// using consistent hashing.
//
// Servers may be added and deleted at any time via AddServer()
// and DeleteServer() functions.
//
// The client is goroutine-safe.
type DistributedClient struct {
	// The number of simultaneous TCP connections to establish
	// to each memcached server.
	//
	// The client is able to squeeze out impossible from a single
	// connection by pipelining a ton of requests on it.
	// Multiple simultaneous connections may be required in the following
	// cases:
	//   * If memcached server delays incoming requests' execution.
	//     Since memcached protocol doesn't allow out-of-order requests'
	//     execution, a single slow request may delay execution of all
	//     the requests pipelined on the connection after it.
	//     Multiple concurrent connections may help in such a situation.
	//   * If memcached server runs on multi-CPU system, but uses a single
	//     CPU (thread) per connection.
	ConnectionsCount int

	// The maximum number of pending requests awaiting to be processed
	// by each memcached server.
	MaxPendingRequestsCount int

	// The size in bytes of buffer used by the client for reading responses
	// received from memcached per connection.
	ReadBufferSize int

	// The size in bytes of buffer used by the Client for writing requests
	// to be sent to memcached per connection.
	WriteBufferSize int

	// The size in bytes of OS-supplied read buffer per TCP connection.
	OSReadBufferSize int

	// The size in bytes of OS-supplied write buffer per TCP connection.
	OSWriteBufferSize int

	lock        sync.Mutex
	clientsList []*Client
	clientsMap  map[string]*Client
	clientsHash consistentHash
}

func (c *DistributedClient) Start() {
	if c.clientsMap != nil {
		panic("Did you forgot calling DistributedClient.Stop()?")
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	c.clientsMap = make(map[string]*Client)
	c.clientsHash.ReplicasCount = consistentHashReplicasCount
	c.clientsHash.BucketsCount = consistentHashBucketsCount
	c.clientsHash.Init()
}

func (c *DistributedClient) Stop() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, client := range c.clientsList {
		client.Stop()
	}

	c.clientsList = nil
	c.clientsMap = nil
}

func (c *DistributedClient) registerClient(client *Client) bool {
	connectAddr := client.ConnectAddr

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.clientsMap[connectAddr] != nil {
		return false
	}

	c.clientsList = append(c.clientsList, client)
	c.clientsMap[connectAddr] = client

	clientIdx := len(c.clientsList) - 1
	c.clientsHash.Add([]byte(connectAddr), clientIdx)
	return true
}

func lookupClientIdx(clients []*Client, client *Client) int {
	clientsCount := len(clients)
	for i := 0; i < clientsCount; i++ {
		if clients[i] == client {
			return i
		}
	}
	panic("Tere is no the given client in the clients list")
}

func (c *DistributedClient) deregisterClient(connectAddr string) *Client {
	c.lock.Lock()
	defer c.lock.Unlock()

	client := c.clientsMap[connectAddr]
	if client != nil {
		clientIdx := lookupClientIdx(c.clientsList, client)
		c.clientsList = append(c.clientsList[:clientIdx], c.clientsList[clientIdx+1:]...)
		c.clientsHash.Delete([]byte(connectAddr))
		delete(c.clientsMap, connectAddr)
	}
	return client
}

func (c *DistributedClient) AddServer(connectAddr string) {
	client := &Client{
		ConnectAddr:             connectAddr,
		ConnectionsCount:        c.ConnectionsCount,
		MaxPendingRequestsCount: c.MaxPendingRequestsCount,
		ReadBufferSize:          c.ReadBufferSize,
		WriteBufferSize:         c.WriteBufferSize,
		OSReadBufferSize:        c.OSReadBufferSize,
		OSWriteBufferSize:       c.OSWriteBufferSize,
	}
	client.Start()

	if !c.registerClient(client) {
		client.Stop()
		return
	}
}

func (c *DistributedClient) DeleteServer(connectAddr string) {
	client := c.deregisterClient(connectAddr)
	if client == nil {
		return
	}

	client.Stop()
}

func (c *DistributedClient) clientIdx(key []byte) int {
	return c.clientsHash.Get(key).(int)
}

func (c *DistributedClient) clientNolock(key []byte) *Client {
	clientIdx := c.clientIdx(key)
	return c.clientsList[clientIdx]
}

func (c *DistributedClient) clientsCount() int {
	if c.clientsMap == nil {
		panic("Did you fogot calling DistributedClient.Start()?")
	}
	return len(c.clientsList)
}

func (c *DistributedClient) client(key []byte) (client *Client, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.clientsCount() == 0 {
		err = ErrNoServers
		return
	}
	client = c.clientNolock(key)
	return
}

func (c *DistributedClient) itemsPerClient(items []Item) (m [][]Item, clients []*Client, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	clientsCount := c.clientsCount()
	if clientsCount == 0 {
		err = ErrNoServers
		return
	}

	m = make([][]Item, clientsCount)
	for _, item := range items {
		clientIdx := c.clientIdx(item.Key)
		m[clientIdx] = append(m[clientIdx], item)
	}
	clients = make([]*Client, clientsCount)
	copy(clients, c.clientsList)
	return
}

func (c *DistributedClient) GetMulti(items []Item) error {
	itemsPerClient, clients, err := c.itemsPerClient(items)
	if err != nil {
		return err
	}

	for clientIdx, clientItems := range itemsPerClient {
		if err = clients[clientIdx].GetMulti(clientItems); err != nil {
			return err
		}
	}
	return nil
}

func (c *DistributedClient) Get(item *Item) error {
	client, err := c.client(item.Key)
	if err != nil {
		return err
	}
	return client.Get(item)
}

func (c *DistributedClient) Cget(item *Citem) error {
	client, err := c.client(item.Key)
	if err != nil {
		return err
	}
	return client.Cget(item)
}

func (c *DistributedClient) GetDe(item *Item, graceDuration time.Duration) error {
	client, err := c.client(item.Key)
	if err != nil {
		return err
	}
	return client.GetDe(item, graceDuration)
}

func (c *DistributedClient) Set(item *Item) error {
	client, err := c.client(item.Key)
	if err != nil {
		return err
	}
	return client.Set(item)
}

func (c *DistributedClient) Cset(item *Citem) error {
	client, err := c.client(item.Key)
	if err != nil {
		return err
	}
	return client.Cset(item)
}

func (c *DistributedClient) SetNowait(item *Item) {
	client, err := c.client(item.Key)
	if err == nil {
		client.SetNowait(item)
	}
}

func (c *DistributedClient) CsetNowait(item *Citem) {
	client, err := c.client(item.Key)
	if err == nil {
		client.CsetNowait(item)
	}
}

func (c *DistributedClient) Delete(key []byte) error {
	client, err := c.client(key)
	if err != nil {
		return err
	}
	return client.Delete(key)
}

func (c *DistributedClient) DeleteNowait(key []byte) {
	client, err := c.client(key)
	if err == nil {
		client.DeleteNowait(key)
	}
}

func (c *DistributedClient) allClients() (clients []*Client, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	clientsCount := c.clientsCount()
	if clientsCount == 0 {
		err = ErrNoServers
		return
	}

	clients = make([]*Client, clientsCount)
	copy(clients, c.clientsList)
	return
}

func (c *DistributedClient) FlushAllDelayed(expiration time.Duration) error {
	clients, err := c.allClients()
	if err != nil {
		return err
	}
	for _, client := range clients {
		if err := client.FlushAllDelayed(expiration); err != nil {
			return err
		}
	}
	return nil
}

func (c *DistributedClient) FlushAll() error {
	clients, err := c.allClients()
	if err != nil {
		return err
	}
	for _, client := range clients {
		if err := client.FlushAll(); err != nil {
			return err
		}
	}
	return nil
}

func (c *DistributedClient) FlushAllDelayedNowait(expiration time.Duration) {
	clients, err := c.allClients()
	if err != nil {
		return
	}
	for _, client := range clients {
		client.FlushAllDelayedNowait(expiration)
	}
}

func (c *DistributedClient) FlushAllNowait() {
	clients, err := c.allClients()
	if err != nil {
		return
	}
	for _, client := range clients {
		client.FlushAllNowait()
	}
}
