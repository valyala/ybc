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
	ErrNoServers = errors.New("memcache.DistributedClient: there are no registered servers")
)

// Memcache client, which can shard requests to multiple servers
// using consistent hashing.
//
// Servers may be dynamically added and deleted at any time via AddServer()
// and DeleteServer() functions if the client is started via Start()
// call.
//
// The client is goroutine-safe.
//
// Usage:
//
//   c := DistributedClient{}
//   c.StartStatic([]string{"host1:11211", "host2:11211", "host3:11211"})
//   defer c.Stop()
//
//   item := Item{
//       Key:   []byte("key"),
//       Value: []byte("value"),
//   }
//   if err := c.Set(&item); err != nil {
//       handleError(err)
//   }
//   if err := c.Get(&item); err != nil {
//       handleError(err)
//   }
//
type DistributedClient struct {
	ClientConfig

	isDynamic   bool
	mutex       sync.Mutex
	clientsList []*Client
	clientsMap  map[string]*Client
	clientsHash consistentHash
}

func (c *DistributedClient) lock() {
	if c.isDynamic {
		c.mutex.Lock()
	}
}

func (c *DistributedClient) unlock() {
	if c.isDynamic {
		c.mutex.Unlock()
	}
}

func (c *DistributedClient) init(isDynamic bool) {
	c.isDynamic = isDynamic

	c.lock()
	defer c.unlock()

	if c.clientsMap != nil {
		panic("Did you forgot calling DistributedClient.Stop() before calling DistributedClient.Start()?")
	}
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

	c.lock()
	defer c.unlock()

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
		ServerAddr:   serverAddr,
		ClientConfig: c.ClientConfig,
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
	c.lock()
	defer c.unlock()

	if c.clientsMap == nil {
		panic("Did you forgot calling DistributedClient.Start() before calling DistributedClient.Stop()?")
	}
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
	panic("DistributedClient: tere is no the given client in the clients list")
}

func (c *DistributedClient) deregisterClient(serverAddr string) *Client {
	c.lock()
	defer c.unlock()

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
// via DistributedClient.Start() call,
// not via DistributedClient.StartStatic() call!
//
// Added servers may be removed at any time
// via DistributedClient.DeleteServer() call.
func (c *DistributedClient) AddServer(serverAddr string) {
	if !c.isDynamic {
		panic("DistributedClient.AddServer() cannot be called from static client!")
	}
	c.addServer(serverAddr)
}

// Dynamically removes the given server from the client.
//
// serverAddr must be in the form 'host:port'
//
// This function may be called only if the client has been started
// via DistributedClient.Start() call,
// not via DistributedClient.StartStatic() call!
func (c *DistributedClient) DeleteServer(serverAddr string) {
	if !c.isDynamic {
		panic("DistributedClient.DeleteServer() cannot be called from static client!")
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

func (c *DistributedClient) clientsCount() (n int, err error) {
	if c.clientsMap == nil {
		err = ErrClientNotRunning
		return
	}
	n = len(c.clientsList)
	if n == 0 {
		err = ErrNoServers
	}
	return
}

func (c *DistributedClient) client(key []byte) (client *Client, err error) {
	c.lock()
	// do not use defer c.unlock() for performance reasons.

	if _, err = c.clientsCount(); err != nil {
		c.unlock()
		return
	}
	client = c.clientNolock(key)
	c.unlock()
	return
}

func (c *DistributedClient) itemsPerClient(items []Item) (m [][]Item, clients []*Client, err error) {
	c.lock()
	// do not use defer c.unlock() for performance reasons.

	clientsCount, err := c.clientsCount()
	if err != nil {
		c.unlock()
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
	c.unlock()
	return
}

func handleRaceCondition(err *error) {
	if r := recover(); r != nil {
		*err = ErrClientNotRunning
	}
}

// See Client.GetMulti().
func (c *DistributedClient) GetMulti(items []Item) (err error) {
	itemsPerClient, clients, err := c.itemsPerClient(items)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	for clientIdx, clientItems := range itemsPerClient {
		if err = clients[clientIdx].GetMulti(clientItems); err != nil {
			return
		}
	}
	return
}

// See Client.Get().
func (c *DistributedClient) Get(item *Item) (err error) {
	client, err := c.client(item.Key)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	return client.Get(item)
}

// See Client.Cget().
func (c *DistributedClient) Cget(item *Item) (err error) {
	client, err := c.client(item.Key)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	return client.Cget(item)
}

// See Client.GetDe().
func (c *DistributedClient) GetDe(item *Item, graceDuration time.Duration) (err error) {
	client, err := c.client(item.Key)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	return client.GetDe(item, graceDuration)
}

// See Client.CgetDe()
func (c *DistributedClient) CgetDe(item *Item, graceDuration time.Duration) (err error) {
	client, err := c.client(item.Key)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	return client.CgetDe(item, graceDuration)
}

// See Client.Set().
func (c *DistributedClient) Set(item *Item) (err error) {
	client, err := c.client(item.Key)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	return client.Set(item)
}

// See Client.Add().
func (c *DistributedClient) Add(item *Item) (err error) {
	client, err := c.client(item.Key)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	return client.Add(item)
}

// See Client.Cas()
func (c *DistributedClient) Cas(item *Item) (err error) {
	client, err := c.client(item.Key)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	return client.Cas(item)
}

// See Client.SetNowait().
func (c *DistributedClient) SetNowait(item *Item) {
	client, err := c.client(item.Key)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	client.SetNowait(item)
}

// See Client.Delete().
func (c *DistributedClient) Delete(key []byte) (err error) {
	client, err := c.client(key)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	return client.Delete(key)
}

// See Client.DeleteNowait().
func (c *DistributedClient) DeleteNowait(key []byte) {
	client, err := c.client(key)
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	client.DeleteNowait(key)
}

func (c *DistributedClient) allClients() (clients []*Client, err error) {
	c.lock()
	// do not use defer c.unlock() for performance reasons.

	clientsCount, err := c.clientsCount()
	if err != nil {
		c.unlock()
		return
	}
	if c.isDynamic {
		clients = make([]*Client, clientsCount)
		copy(clients, c.clientsList)
	} else {
		clients = c.clientsList
	}
	c.unlock()
	return
}

// See Client.FlushAllDelayed().
func (c *DistributedClient) FlushAllDelayed(expiration time.Duration) (err error) {
	clients, err := c.allClients()
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	for _, client := range clients {
		if err = client.FlushAllDelayed(expiration); err != nil {
			return
		}
	}
	return
}

// See Client.FlushAll().
func (c *DistributedClient) FlushAll() (err error) {
	clients, err := c.allClients()
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	for _, client := range clients {
		if err = client.FlushAll(); err != nil {
			return
		}
	}
	return
}

// See Client.FlushAllDelayedNowait().
func (c *DistributedClient) FlushAllDelayedNowait(expiration time.Duration) {
	clients, err := c.allClients()
	if err != nil {
		return
	}
	if c.isDynamic {
		defer handleRaceCondition(&err)
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
	if c.isDynamic {
		defer handleRaceCondition(&err)
	}
	for _, client := range clients {
		client.FlushAllNowait()
	}
}
