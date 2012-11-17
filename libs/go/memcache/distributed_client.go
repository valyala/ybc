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
// Servers may be dynamically added and deleted at any time via AddServer()
// and DeleteServer() functions if the client is started via Start()
// call.
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

	isDynamic   bool
	lock        sync.Mutex
	clientsList []*Client
	clientsMap  map[string]*Client
	clientsHash consistentHash
}

func (c *DistributedClient) init(isDynamic bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.clientsMap != nil {
		panic("Did you forgot calling DistributedClient.Stop()?")
	}
	c.isDynamic = isDynamic
	c.clientsMap = make(map[string]*Client)
	c.clientsHash.ReplicasCount = consistentHashReplicasCount
	c.clientsHash.BucketsCount = consistentHashBucketsCount
	c.clientsHash.Init()
}

// Starts distributed client with the ability to dynamically add/remove servers
// via DistributedClient.AddServer() and DistributedClient.DeleteServer().
//
// Started client must be stopped via c.Stop() call when no longer needed!
//
// Use DistributedClient.StartStatic() if you don't plan dynamically
// adding/removing servers to/from the client. The resulting static client
// may work faster than the dynamic client.
func (c *DistributedClient) Start() {
	c.init(true)
}

func (c *DistributedClient) registerClient(client *Client) bool {
	serverAddr := client.ServerAddr

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.clientsMap[serverAddr] != nil {
		return false
	}
	c.clientsList = append(c.clientsList, client)
	c.clientsMap[serverAddr] = client

	clientIdx := len(c.clientsList) - 1
	c.clientsHash.Add([]byte(serverAddr), clientIdx)
	return true
}

func (c *DistributedClient) addServer(serverAddr string) {
	client := &Client{
		ServerAddr:              serverAddr,
		ConnectionsCount:        c.ConnectionsCount,
		MaxPendingRequestsCount: c.MaxPendingRequestsCount,
		ReadBufferSize:          c.ReadBufferSize,
		WriteBufferSize:         c.WriteBufferSize,
		OSReadBufferSize:        c.OSReadBufferSize,
		OSWriteBufferSize:       c.OSWriteBufferSize,
	}
	if c.registerClient(client) {
		client.Start()
	}
}

// Starts distributed client connected to the given memcache servers.
//
// Each serverAddr must be in the form 'host:port'.
//
// Started client must be stopped via DistributedClient.Stop() call
// when no longer needed.
//
// Use DistributedClient.Start() if you plan dynamically adding/removing servers
// to/from the client. Note that the resuling dynamic client may work
// a bit slower than the static client.
func (c *DistributedClient) StartStatic(serverAddrs []string) {
	c.init(false)
	for _, serverAddr := range serverAddrs {
		c.addServer(serverAddr)
	}
}

// Stops distributed client.
func (c *DistributedClient) Stop() {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, client := range c.clientsList {
		client.Stop()
	}

	c.clientsList = nil
	c.clientsMap = nil
}

func lookupClientIdx(clients []*Client, client *Client) int {
	for i, c := range clients {
		if c == client {
			return i
		}
	}
	panic("Tere is no the given client in the clients list")
}

func (c *DistributedClient) deregisterClient(serverAddr string) *Client {
	c.lock.Lock()
	defer c.lock.Unlock()

	client := c.clientsMap[serverAddr]
	if client != nil {
		clientIdx := lookupClientIdx(c.clientsList, client)
		c.clientsList = append(c.clientsList[:clientIdx], c.clientsList[clientIdx+1:]...)
		c.clientsHash.Delete([]byte(serverAddr))
		delete(c.clientsMap, serverAddr)
	}
	return client
}

// Dynamically adds the given server to the client.
//
// serverAddr must be in the form 'host:port'.
//
// This function may be called only if the client has been started
// via DistributedClient.Start() call, not via DistributedClient.StartStatic() call!
//
// Added servers may be removed at any time
// via DistributedClient.DeleteServer() call.
func (c *DistributedClient) AddServer(serverAddr string) {
	if !c.isDynamic {
		panic("DistributedClient.AddServer() cannot be used in static client!")
	}
	c.addServer(serverAddr)
}

// Dynamically removes the given server from the client.
//
// serverAddr must be in the form 'host:port'
func (c *DistributedClient) DeleteServer(serverAddr string) {
	if !c.isDynamic {
		panic("DistributedClient.DeleteServer() cannot be used in static client!")
	}
	client := c.deregisterClient(serverAddr)
	if client != nil {
		client.Stop()
	}
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
	if c.isDynamic {
		c.lock.Lock()
		defer c.lock.Unlock()
	}
	if c.clientsCount() == 0 {
		err = ErrNoServers
		return
	}
	client = c.clientNolock(key)
	return
}

func (c *DistributedClient) itemsPerClient(items []Item) (m [][]Item, clients []*Client, err error) {
	if c.isDynamic {
		c.lock.Lock()
		defer c.lock.Unlock()
	}
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
	if c.isDynamic {
		clients = make([]*Client, clientsCount)
		copy(clients, c.clientsList)
	} else {
		clients = c.clientsList
	}
	return
}

// See Client.GetMulti().
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

// See Client.Get().
func (c *DistributedClient) Get(item *Item) error {
	client, err := c.client(item.Key)
	if err != nil {
		return err
	}
	return client.Get(item)
}

// See Client.Cget().
func (c *DistributedClient) Cget(item *Citem) error {
	client, err := c.client(item.Key)
	if err != nil {
		return err
	}
	return client.Cget(item)
}

// See Client.GetDe().
func (c *DistributedClient) GetDe(item *Item, graceDuration time.Duration) error {
	client, err := c.client(item.Key)
	if err != nil {
		return err
	}
	return client.GetDe(item, graceDuration)
}

// See Client.Set().
func (c *DistributedClient) Set(item *Item) error {
	client, err := c.client(item.Key)
	if err != nil {
		return err
	}
	return client.Set(item)
}

// See Client.Cset().
func (c *DistributedClient) Cset(item *Citem) error {
	client, err := c.client(item.Key)
	if err != nil {
		return err
	}
	return client.Cset(item)
}

// See Client.SetNowait().
func (c *DistributedClient) SetNowait(item *Item) {
	client, err := c.client(item.Key)
	if err == nil {
		client.SetNowait(item)
	}
}

// See Client.CsetNowait().
func (c *DistributedClient) CsetNowait(item *Citem) {
	client, err := c.client(item.Key)
	if err == nil {
		client.CsetNowait(item)
	}
}

// See Client.Delete().
func (c *DistributedClient) Delete(key []byte) error {
	client, err := c.client(key)
	if err != nil {
		return err
	}
	return client.Delete(key)
}

// See Client.DeleteNowait().
func (c *DistributedClient) DeleteNowait(key []byte) {
	client, err := c.client(key)
	if err == nil {
		client.DeleteNowait(key)
	}
}

func (c *DistributedClient) allClients() (clients []*Client, err error) {
	if c.isDynamic {
		c.lock.Lock()
		defer c.lock.Unlock()
	}
	clientsCount := c.clientsCount()
	if clientsCount == 0 {
		err = ErrNoServers
		return
	}
	if c.isDynamic {
		clients = make([]*Client, clientsCount)
		copy(clients, c.clientsList)
	} else {
		clients = c.clientsList
	}
	return
}

// See Client.FlushAllDelayed().
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

// See Client.FlushAll().
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

// See Client.FlushAllDelayedNowait().
func (c *DistributedClient) FlushAllDelayedNowait(expiration time.Duration) {
	clients, err := c.allClients()
	if err != nil {
		return
	}
	for _, client := range clients {
		client.FlushAllDelayedNowait(expiration)
	}
}

// See Client.FlushAllNowait().
func (c *DistributedClient) FlushAllNowait() {
	clients, err := c.allClients()
	if err != nil {
		return
	}
	for _, client := range clients {
		client.FlushAllNowait()
	}
}
