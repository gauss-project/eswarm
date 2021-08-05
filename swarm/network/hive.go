// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package network

import (
	"fmt"
	"github.com/gauss-project/eswarm/common"
	"sync"
	"time"

	"github.com/gauss-project/eswarm/common/hexutil"
	"github.com/gauss-project/eswarm/p2p"
	"github.com/gauss-project/eswarm/p2p/enode"
	"github.com/gauss-project/eswarm/swarm/log"
	"github.com/gauss-project/eswarm/swarm/state"
)
type CacheItem struct {
	k string
	v interface{}
}
type ExpirableCache struct {
	defaultTimeout time.Duration
	kv sync.Map
	runner *time.Ticker
	kvMutex sync.Mutex
	onExpired func(string)
}

func NewExpirableCache(timeout time.Duration) *ExpirableCache {

	val := &ExpirableCache{
		defaultTimeout:timeout,
		runner:time.NewTicker(time.Minute),
	}

	go func() {
		for range val.runner.C {
			val.checkExpired()
		}
	}()

	return val
}

func (e *ExpirableCache)checkExpired(){

	e.kvMutex.Lock()
	defer  e.kvMutex.Unlock()
	expiredKey := make([]string,0)
	e.kv.Range(func(key, value interface{}) bool {
		if  value.(time.Time).Before(time.Now()) {
			expiredKey = append(expiredKey,key.(string))

		}
		return true
	})
	if e.onExpired!= nil {
		for _, key := range expiredKey {
			e.kv.Delete(key)

			e.onExpired(key)
		}
	}else{
		for _, key := range expiredKey {
			e.kv.Delete(key)

		}
	}
}
func (e *ExpirableCache)Close(){
	e.runner.Stop()
}
func (e *ExpirableCache)Reset(key string){
	e.kvMutex.Lock()
	defer  e.kvMutex.Unlock()
	e.kv.Store(key,time.Now().Add(e.defaultTimeout))
}
func (e *ExpirableCache)OnExpired(onExpired func (string)){
	e.onExpired = onExpired
}
/*
Hive is the logistic manager of the swarm

When the hive is started, a forever loop is launched that
asks the  kademlia nodetable
to suggest peers to bootstrap connectivity
*/
/**
Hive是节点启动器
Hive是swarm的对数距离管理器，当HIVE启动后，启动一个无限循环，请求Kademlia节点表，寻找一个启动的连接
*/
// HiveParams holds the config options to hive
type HiveParams struct {
	Discovery             bool  // if want discovery of not
	PeersBroadcastSetSize uint8 // how many peers to use when relaying
	MaxPeersPerRequest    uint8 // max size for peer address batches
	KeepAliveInterval     time.Duration
	RefreshPeers          time.Duration
}

// NewHiveParams returns hive config with only the
func NewHiveParams() *HiveParams {
	return &HiveParams{
		Discovery:             true,
		PeersBroadcastSetSize: 3,
		MaxPeersPerRequest:    5,
		KeepAliveInterval:     500 * time.Millisecond,
		RefreshPeers:          120 * time.Second,
	}
}

// Hive manages network connections of the swarm node
type Hive struct {
	*HiveParams                    // settings
	*Kademlia                      // the overlay connectiviy driver
	Store        state.Store       // storage interface to save peers across sessions
	addPeer      func(*enode.Node) // server callback to connect to a peer
	getKnowNodes func() []*enode.Node
	addNode      func(node *enode.Node)
	removePeer   func(node *enode.Node)
	// bookkeeping
	lock        sync.Mutex
	refreshLock sync.Mutex
	cacheLock   sync.Mutex
	peers       map[enode.ID]*BzzPeer

	ticker        *time.Ticker
	refreshTicker *time.Ticker
	idleNodes     *ExpirableCache
	newNodeDiscov chan struct{}
	server		 *p2p.Server
	quitC         chan struct{}
}

// NewHive constructs a new hive
// HiveParams: config parameters
// Kademlia: connectivity driver using a network topology
// StateStore: to save peers across sessions
func NewHive(params *HiveParams, kad *Kademlia, store state.Store) *Hive {
	hive := &Hive{
		HiveParams:    params,
		Kademlia:      kad,
		Store:         store,
		peers:         make(map[enode.ID]*BzzPeer),
		newNodeDiscov: make(chan struct{}),
		quitC:         make(chan struct{}),
		idleNodes:NewExpirableCache(10*time.Minute),
	}



	return hive
}
/**
	Rece
 */
func (h *Hive) 	OnNewReceipts(address common.Address,id enode.ID,length int){
	h.cacheLock.Lock()
	defer h.cacheLock.Unlock()
	h.lock.Lock()
	peer := h.peers[id]
	h.lock.Unlock()
	if peer != nil && enode.IsLightNode(enode.NodeTypeOption(peer.Node().NodeType())) {

		h.idleNodes.Reset(id.String())
	}

}
func (h *Hive) ID() string {
	return "KADEMLIA"
}
// Start stars the hive, receives p2p.Server only at startup
// server is used to connect to a peer based on its NodeID or enode URL
// these are called on the p2p.Server which runs on the node
func (h *Hive) Start(server *p2p.Server) error {
	log.Info("Starting Hive", "BaseAddr", fmt.Sprintf("%x", h.BaseAddr()[:4]))
	// if state store is specified, load peers to prepopulate the overlay address book
	if h.Store != nil {
		log.Info("Detected an existing store. trying to load peers")
		if err := h.loadPeers(); err != nil {
			log.Error(fmt.Sprintf("%08x hive encoutered an error trying to load peers", h.BaseAddr()[:4]))
			return err
		}
	}
	// assigns the p2p.Server#AddPeer function to connect to peers
	h.addPeer = server.AddPeer
	h.removePeer = server.RemovePeer
	h.getKnowNodes = server.GetKnownNodesSorted
	// ticker to keep the hive alive
	h.ticker = time.NewTicker(h.KeepAliveInterval)
	h.refreshTicker = time.NewTicker(h.RefreshPeers)
	server.SetNotificationChan(h.newNodeDiscov)
	h.server =server
	h.Kademlia.SetFilter(server.FilterChain)
	//server.SetNodeAddChecker(h.CheckNode)
	// this loop is doing bootstrapping and maintains a healthy table
	h.doRefresh()
	h.idleNodes.OnExpired(func(s string) {
		nodeId := enode.HexID(s)
		h.lock.Lock()

		peer := h.peers[nodeId]
		h.lock.Unlock()
		if peer != nil {
			log.Trace("drop idle connection","id",peer.ID() )
			peer.Disconnect(p2p.DiscIdleConnection)
		}
	})
	go h.connect()
	go h.refresh()
	return nil
}
func (h *Hive) CheckNode(n *enode.Node) bool {
	/*length,po := h.Kademlia.GetIntendBinInfo(n)
	if  length >= h.KadParams.MaxBinSize {
		log.Info("bucket size full :","id",n.ID(),"bucket",po,"connected",length)
		return false
	}else{
		log.Info("bucket add ok :","id",n.ID(),"bucket",po,"connected",length)
		return true
	}*/
	return true
}

func (h *Hive) refresh() {
	for {
		select {
		case <-h.refreshTicker.C:
			//log.Debug(" Refresh ticker ")
			go h.doRefresh()
		case <-h.newNodeDiscov:
			log.Debug(" New Node notified")
			go h.doRefresh()
			//log.Debug(" New Node processed")
		case <-h.quitC:
			if h.newNodeDiscov != nil {
				close(h.newNodeDiscov)
			}

			return

		}

	}
}

//refresh load peers and register to kad network
func (h *Hive) doRefresh() {
	//log.Debug("do Refresh")

	nodes := make([]*BzzAddr, 0)
	knownNodes := h.getKnowNodes()
	for _, node := range knownNodes {
		nodes = append(nodes, NewAddr(node))
	}
	//log.Info("hive doRefresh lock")
	//defer log.Info("hive doRefresh unlock")
	h.refreshLock.Lock()

	defer h.refreshLock.Unlock()
	h.Register(nodes...)
	h.savePeers()

}

// Stop terminates the updateloop and saves the peers
func (h *Hive) Stop() error {
	h.server.SetNotificationChan(nil)
	h.idleNodes.Close()
	log.Info(fmt.Sprintf("%08x hive stopping, saving peers", h.BaseAddr()[:4]))
	if h.ticker != nil {
		h.ticker.Stop()
	}
	if h.refreshTicker != nil {
		h.refreshTicker.Stop()
	}
	close(h.newNodeDiscov)
	h.newNodeDiscov = nil
	close(h.quitC)
	if h.Store != nil {
		if err := h.savePeers(); err != nil {
			return fmt.Errorf("could not save peers to persistence store: %v", err)
		}
		if err := h.Store.Close(); err != nil {
			return fmt.Errorf("could not close file handle to persistence store: %v", err)
		}
	}
	log.Info(fmt.Sprintf("%08x hive stopped, dropping peers", h.BaseAddr()[:4]))
	h.EachConn(nil, 255, func(p *Peer, _ int) bool {
		log.Info(fmt.Sprintf("%08x dropping peer %08x", h.BaseAddr()[:4], p.Address()[:4]))
		p.Drop(nil)
		return true
	})

	log.Info(fmt.Sprintf("%08x all peers dropped", h.BaseAddr()[:4]))
	return nil
}

// connect is a forever loop
// at each iteration, ask the overlay driver to suggest the most preferred peer to connect to
// as well as advertises saturation depth if needed
func (h *Hive) connect() {
	for range h.ticker.C {

		addr, depth, changed := h.SuggestPeer()
		if h.Discovery && changed {
			NotifyDepth(uint8(depth), h.Kademlia)
		}
		if addr == nil {
			continue
		}

		log.Trace(fmt.Sprintf("%08x hive connect() suggested %08x", h.BaseAddr()[:4], addr.Address()[:4]))
		under, err := enode.ParseV4(string(addr.Under()))
		if err != nil {
			log.Warn(fmt.Sprintf("%08x unable to connect to bee %08x: invalid node URL: %v", h.BaseAddr()[:4], addr.Address()[:4], err))
			continue
		}
		log.Trace(fmt.Sprintf("%08x attempt to connect to bee %08x", h.BaseAddr()[:4], addr.Address()[:4]))
		h.addPeer(under)
	}
	log.Info("Hive exist")

}

// Run protocol run function
func (h *Hive) Run(p *BzzPeer) error {

	h.trackPeer(p)
	defer h.untrackPeer(p)
	dp := NewPeer(p, h.Kademlia)


	depth, changed, err := h.On(dp)
	if err != nil {
		return err
	}

	// if we want discovery, advertise change of depth
	if h.Discovery {
		if changed {
			// if depth changed, send to all peers
			NotifyDepth(depth, h.Kademlia)
		} else {
			// otherwise just send depth to new peer
			dp.NotifyDepth(depth)
		}
		aNode, _ := enode.ParseV4(string(p.BzzAddr.UAddr))
		if aNode != nil && enode.GetRetrievalOptions(enode.NodeTypeOption(aNode.NodeType())) == enode.RetrievalEnabled {
			NotifyPeer(p.BzzAddr, h.Kademlia)
		}

	}
	defer h.Off(dp)
	//if enode.IsLightNode(enode.NodeTypeOption(dp.NodeType())) {
		//轻节点，模拟一个receipt动作，激活
		h.OnNewReceipts(common.Address{0},dp.ID(),1)
	//}

	return dp.Run(dp.HandleMsg)

}

func (h *Hive) trackPeer(p *BzzPeer) {
	h.lock.Lock()
	h.peers[p.ID()] = p
	h.lock.Unlock()
}

func (h *Hive) untrackPeer(p *BzzPeer) {
	h.lock.Lock()
	h.removePeer(p.Node())
	delete(h.peers, p.ID())
	h.lock.Unlock()
}

// NodeInfo function is used by the p2p.server RPC interface to display
// protocol specific node information
func (h *Hive) NodeInfo() interface{} {
	return h.String()
}

// PeerInfo function is used by the p2p.server RPC interface to display
// protocol specific information any connected peer referred to by their NodeID
func (h *Hive) PeerInfo(id enode.ID) interface{} {
	h.lock.Lock()
	p := h.peers[id]
	h.lock.Unlock()

	if p == nil {
		return nil
	}
	addr := NewAddr(p.Node())
	return struct {
		OAddr hexutil.Bytes
		UAddr hexutil.Bytes
	}{
		OAddr: addr.OAddr,
		UAddr: addr.UAddr,
	}
}

// loadPeers, savePeer implement persistence callback/
func (h *Hive) loadPeers() error {
	var as []*BzzAddr
	err := h.Store.Get("peers", &as)
	if err != nil {
		if err == state.ErrNotFound {
			log.Info(fmt.Sprintf("hive %08x: no persisted peers found", h.BaseAddr()[:4]))
			return nil
		}
		return err
	}
	log.Info(fmt.Sprintf("hive %08x: peers loaded", h.BaseAddr()[:4]))

	return h.Register(as...)
}

// savePeers, savePeer implement persistence callback/
func (h *Hive) savePeers() error {
	var peers []*BzzAddr
	h.Kademlia.EachConn(nil, 256, func(pa *Peer, i int) bool {
		if pa == nil {
			log.Warn(fmt.Sprintf("empty addr: %v", i))
			return true
		}
		log.Trace("saving peer", "peer", pa)
		peers = append(peers, pa.BzzAddr)
		return true
	})
	if err := h.Store.Put("peers", peers); err != nil {
		return fmt.Errorf("could not save peers: %v", err)
	}
	return nil
}
