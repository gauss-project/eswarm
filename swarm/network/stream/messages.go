// Copyright 2018 The go-ethereum Authors
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

package stream

import (
	"context"
	"fmt"
	"github.com/gauss-project/eswarm/swarm/storage"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/gauss-project/eswarm/metrics"
	"github.com/gauss-project/eswarm/swarm/log"
	bv "github.com/gauss-project/eswarm/swarm/network/bitvector"
	"github.com/gauss-project/eswarm/swarm/spancontext"
)

var syncBatchTimeout = 30 * time.Second

// Stream defines a unique stream identifier.
type Stream struct {
	// Name is used for Client and Server functions identification.
	Name string
	// Key is the name of specific stream data.
	Key string
	// Live defines whether the stream delivers only new data
	// for the specific stream.
	Live bool
}

func NewStream(name string, key string, live bool) Stream {
	return Stream{
		Name: name,
		Key:  key,
		Live: live,
	}
}

// String return a stream id based on all Stream fields.
func (s Stream) String() string {
	t := "h"
	if s.Live {
		t = "l"
	}
	return fmt.Sprintf("%s|%s|%s", s.Name, s.Key, t)
}

// SubcribeMsg is the protocol msg for requesting a stream(section)
type SubscribeMsg struct {
	Stream   Stream
	History  *Range `rlp:"nil"`
	Priority uint8  // delivered on priority channel
}

// RequestSubscriptionMsg is the protocol msg for a node to request subscription to a
// specific stream
type RequestSubscriptionMsg struct {
	Stream   Stream
	History  *Range `rlp:"nil"`
	Priority uint8  // delivered on priority channel
}

func (p *Peer) handleRequestSubscription(ctx context.Context, req *RequestSubscriptionMsg) (err error) {
	log.Debug(fmt.Sprintf("handleRequestSubscription: streamer %s to subscribe to %s with stream %s", p.streamer.addr, p.ID(), req.Stream))
	if err = p.streamer.Subscribe(p.ID(), req.Stream, req.History, req.Priority); err != nil {
		// The error will be sent as a subscribe error message
		// and will not be returned as it will prevent any new message
		// exchange between peers over p2p. Instead, error will be returned
		// only if there is one from sending subscribe error message.
		err = p.Send(ctx, SubscribeErrorMsg{
			Error: err.Error(),
		})
	}
	return err
}

//处理订阅消息
func (p *Peer) handleSubscribeMsg(ctx context.Context, req *SubscribeMsg) (err error) {
	metrics.GetOrRegisterCounter("peer.handlesubscribemsg", nil).Inc(1)

	defer func() {
		if err != nil {
			// The error will be sent as a subscribe error message
			// and will not be returned as it will prevent any new message
			// exchange between peers over p2p. Instead, error will be returned
			// only if there is one from sending subscribe error message.
			err = p.Send(context.TODO(), SubscribeErrorMsg{
				Error: err.Error(),
			})
		}
	}()

	log.Debug("received subscription", "from", p.streamer.addr, "peer", p.ID(), "stream", req.Stream, "history", req.History)

	f, err := p.streamer.GetServerFunc(req.Stream.Name)
	if err != nil {
		return err
	}

	//分析获取相应的streamer
	s, err := f(p, req.Stream.Key, req.Stream.Live)
	if err != nil {
		return err
	}
	//配置流化器
	os, err := p.setServer(req.Stream, s, req.Priority)
	if err != nil {
		return err
	}

	//准备需要读取的范围，如果有，那么就从历史记录那里开始
	var from uint64
	var to uint64
	if !req.Stream.Live && req.History != nil {
		from = req.History.From
		to = req.History.To
	}

	go func() {
		if err := p.SendOfferedHashes(os, from, to); err != nil {
			log.Warn("SendOfferedHashes error", "peer", p.ID().TerminalString(), "err", err)
		}

	}()

	if req.Stream.Live && req.History != nil {
		// subscribe to the history stream
		s, err := f(p, req.Stream.Key, false)
		if err != nil {
			return err
		}

		os, err := p.setServer(getHistoryStream(req.Stream), s, getHistoryPriority(req.Priority))
		if err != nil {
			return err
		}
		go func() {
			if err := p.SendOfferedHashes(os, req.History.From, req.History.To); err != nil {
				log.Warn("SendOfferedHashes error", "peer", p.ID().TerminalString(), "err", err)
			}

		}()
	}

	return nil
}

type SubscribeErrorMsg struct {
	Error string
}

func (p *Peer) handleSubscribeErrorMsg(req *SubscribeErrorMsg) (err error) {
	//TODO the error should be channeled to whoever calls the subscribe
	return fmt.Errorf("subscribe to peer %s: %v", p.ID(), req.Error)
}

type UnsubscribeMsg struct {
	Stream Stream
}

func (p *Peer) handleUnsubscribeMsg(req *UnsubscribeMsg) error {
	return p.removeServer(req.Stream)
}

type QuitMsg struct {
	Stream Stream
}

func (p *Peer) handleQuitMsg(req *QuitMsg) error {
	return p.removeClient(req.Stream)
}

// OfferedHashesMsg is the protocol msg for offering to hand over a
// stream section
type OfferedHashesMsg struct {
	Stream         Stream // name of Stream
	From, To       uint64 // peer and db-specific entry count
	Hashes         []byte // stream of hashes (128)
//	Delayed        uint64
	*HandoverProof        // HandoverProof
}

// String pretty prints OfferedHashesMsg
func (m OfferedHashesMsg) String() string {
	return fmt.Sprintf("Stream '%v' [%v-%v] (%v)", m.Stream, m.From, m.To, len(m.Hashes)/HashSize)
}

// handleOfferedHashesMsg protocol msg handler calls the incoming streamer interface
// Filter method
// 本函数处理收到的offeredHash,其简单的过程就是检查这些hash,看看哪些应该自己存储但是还没有的，通过	bitvector（wantedHash）来汇报给对方，
// 汇报的时候，直接汇报当前需要的哈希和下一次的哈希的范围
// 这里面只是汇报自身所需的哈希，获取的过程是由对方发送过来的，那么：
// 1.设备刚初始化的时候，是不是会占用大量的带宽
// 2.如果某个哈希没有取到（对方因为磁盘空间原因删除了），这个哈希是否就不管了，还是需要到别的地方去取
func (p *Peer) handleOfferedHashesMsg(ctx context.Context, req *OfferedHashesMsg,d *Delivery) error {
	metrics.GetOrRegisterCounter("peer.handleofferedhashes", nil).Inc(1)

	log.Debug("Received offered batch", "peer", p.ID(), "stream", req.Stream, "from", req.From, "to", req.To)
	var sp opentracing.Span
	ctx, sp = spancontext.StartSpan(
		ctx,
		"handle.offered.hashes")
	defer sp.Finish()

	c, _, err := p.getOrSetClient(req.Stream, req.From, req.To)
	var c_live *client
	if !req.Stream.Live {
		c_live, _ = p.clients[Stream{req.Stream.Name, req.Stream.Key, true}]
	}

	if err != nil {
		return err
	}

	hashes := req.Hashes
	lenHashes := len(hashes)
	if lenHashes%HashSize != 0 {
		return fmt.Errorf("error invalid hashes length (len: %v)", lenHashes)
	}

	want, err := bv.New(lenHashes / HashSize)
	if err != nil {
		return fmt.Errorf("error initiaising bitvector of length %v: %v", lenHashes/HashSize, err)
	}

	go func() {
		//首先进行流量控制
		if !d.SyncEnabled() {

			retryTimer := time.NewTicker(20*time.Millisecond);
			for range retryTimer.C {
				if d.SyncEnabled() {

					break;
				}
			}
			retryTimer.Stop()
		}
		//流控结束，可以干活了
		ctr := 0
		errC := make(chan error)
		ctx, cancel := context.WithTimeout(ctx, syncBatchTimeout)

		ctx = context.WithValue(ctx, "source", p.ID().String())
		for i := 0; i < lenHashes; i += HashSize {
			hash := hashes[i : i+HashSize]
			//wait是一个函数，要么是一个fetcher函数，要么是nil(数据已经取了）
			if wait := c.NeedData(ctx, hash); wait != nil {
				ctr++
				want.Set(i/HashSize, true)
				// create request and wait until the chunk data arrives and is stored
				go func(w func(context.Context) error) {
					select {
					case errC <- w(ctx): // 	fetcher函数，从对方去取数据
					case <-ctx.Done(): //
					}
				}(wait)
			}
		}

		//重新建立了一个线程，管理所有的等待事宜
		go func() {
			defer cancel()
			//需要和ctr对应个数的回应
			for i := 0; i < ctr; i++ {
				select {
				case err := <-errC:
					if err != nil {
						log.Debug("client.handleOfferedHashesMsg() error waiting for chunk, dropping peer", "peer", p.ID(), "err", err)
						//p.Drop(err)
						//return
					}
				case <-ctx.Done():
					log.Debug("client.handleOfferedHashesMsg() context done", "ctx.Err()", ctx.Err())
					break
				case <-c.quit:
					log.Debug("client.handleOfferedHashesMsg() quit")
					return
				}
			}
			//没有return出去，说明所有的都正常了(只有errC为0的才是正常的)
			select {
			case c.next <- c.batchDone(p, req, hashes): //这个是正常的退出
			case <-c.quit:
				log.Debug("client.handleOfferedHashesMsg() quit")
			case <-ctx.Done():
				log.Debug("client.handleOfferedHashesMsg() context done", "ctx.Err()", ctx.Err())
			}
		}()
		// only send wantedKeysMsg if all missing chunks of the previous batch arrived
		// except
		if c.stream.Live {
			c.sessionAt = req.From
		}
		//看看还有没有,因为不知道会不会还有，所以总是要发
		from, to := c.nextBatch(req.To+1, c_live)

		log.Debug("set next batch", "peer", p.ID(), "stream", req.Stream, "from", req.From, "to", req.To, "next from", from, "next to", to)
		if from == to {
			log.Debug("Sync finished", "peer", p.ID(), "stream", req.Stream, "from", from, "to", to, "addr", p.streamer.addr)

		}else{
			//注意，from/to变成新的了，Want应该对应的旧的，这个的意思是就同时发送上一次的wantedhash和下一次的	from/to，这个在下面的处理函数里得到了验证要
			msg := &WantedHashesMsg{
				Stream: req.Stream,
				Want:   want.Bytes(),
				From:   from,
				To:     to,
			}
			go func() {

				//		log.Debug("waiting for sending want batch", "peer", p.ID(), "stream", msg.Stream, "from", msg.From, "to", msg.To)
				select {
				case err := <-c.next:
					if err != nil {
						log.Warn("c.next error dropping peer", "err", err)
						//p.Drop(err)
						return
					}
				case <-c.quit:
					log.Debug("client.handleOfferedHashesMsg() quit")
					return
				case <-ctx.Done():
					log.Debug("client.handleOfferedHashesMsg() context done", "ctx.Err()", ctx.Err())
					return
				}
				log.Debug("Sent wanted batch", "peer", p.ID(), "stream", msg.Stream, "from", msg.From, "to", msg.To)
				err := p.SendPriority(ctx, msg, c.priority)
				if err != nil {
					log.Warn("SendPriority error", "err", err)
				}
			}()
		}


	}()

	return nil
}

// WantedHashesMsg is the protocol msg data for signaling which hashes
// offered in OfferedHashesMsg downstream peer actually wants sent over
type WantedHashesMsg struct {
	Stream   Stream
	Want     []byte // bitvector indicating which keys of the batch needed  当前想要的哈希的位映射表
	From, To uint64 // next interval offset - empty if not to be continued  下一个要检查的区域
}

// String pretty prints WantedHashesMsg
func (m WantedHashesMsg) String() string {
	return fmt.Sprintf("Stream '%v', Want: %x, Next: [%v-%v]", m.Stream, m.Want, m.From, m.To)
}

// handleWantedHashesMsg protocol msg handler
// * sends the next batch of unsynced keys
// * sends the actual data chunks as per WantedHashesMsg
func (p *Peer) handleWantedHashesMsg(ctx context.Context, req *WantedHashesMsg) error {
	metrics.GetOrRegisterCounter("peer.handlewantedhashesmsg", nil).Inc(1)

	log.Debug("Received wanted batch", "peer", p.ID(), "stream", req.Stream, "from", req.From, "to", req.To)
	s, err := p.getServer(req.Stream)
	if err != nil {
		return err
	}

	//currentBatch，是当前还没有提供给对端的所有哈希（上一次提供的）
	hashes := s.currentBatch
	// launch in go routine since GetBatch blocks until new hashes arrive
	go func() {
		lastHashTime, ok := p.lastHashTime.Load(req.Stream)
		//lastDelay,ok2 := p.lastDelay.Load(req.Stream)
		if ok {
			timeDelay := 5 * (time.Since(lastHashTime.(time.Time)))
			if timeDelay > 10*time.Second {
				timeDelay = 10 * time.Second
			}
			//	p.lastDelay.Store(req.Stream,  timeDelay)
			delay := time.NewTimer(timeDelay)
			log.Trace("Delayed Sync:", "delayed", timeDelay,"Peer",p.ID(), "stream", req.Stream)
			select {
			case <-delay.C:
			}
		} else {

		}

		p.lastHashTime.Store(req.Stream, time.Now())

		if err := p.SendOfferedHashes(s, req.From, req.To); err != nil {
			log.Warn("SendOfferedHashes error", "peer", p.ID().TerminalString(), "err", err)
		}
	}()
	// go p.SendOfferedHashes(s, req.From, req.To)
	l := len(hashes) / HashSize

	log.Trace("wanted batch length", "peer", p.ID(), "stream", req.Stream, "from", req.From, "to", req.To, "lenhashes", len(hashes), "l", l)
	want, err := bv.NewFromBytes(req.Want, l)
	if err != nil {
		return fmt.Errorf("error initiaising bitvector of length %v: %v", l, err)
	}
	//针对所有的HASH，读取数据后，通过Deliver函数发给对方
	for i := 0; i < l; i++ {
		if want.Get(i) {
			metrics.GetOrRegisterCounter("peer.handlewantedhashesmsg.actualget", nil).Inc(1)

			hash := hashes[i*HashSize : (i+1)*HashSize]
			newCtx := ctx // ,_ := context.WithTimeout(ctx,20*time.Second)
			data, err := s.GetData(newCtx, hash)
			if err == nil {
				//TODO Aegon 重新考虑一下，是否需要继续读取
				chunk := storage.NewChunk(hash, data)
				syncing := true
				if err := p.Deliver(newCtx, chunk, s.priority, syncing); err != nil {

					log.Error("Send deliver data error:", "addr", hash, "error", err)
				}
			} else {
				log.Error("Retrieve deliver data error:", "addr", hash, "error", err)
			}
		}
	}

	return nil
}

// Handover represents a statement that the upstream peer hands over the stream section
type Handover struct {
	Stream     Stream // name of stream
	Start, End uint64 // index of hashes
	Root       []byte // Root hash for indexed segment inclusion proofs
}

// HandoverProof represents a signed statement that the upstream peer handed over the stream section
type HandoverProof struct {
	Sig []byte // Sign(Hash(Serialisation(Handover)))
	*Handover
}

// Takeover represents a statement that downstream peer took over (stored all data)
// handed over
type Takeover Handover

//  TakeoverProof represents a signed statement that the downstream peer took over
// the stream section
type TakeoverProof struct {
	Sig []byte // Sign(Hash(Serialisation(Takeover)))
	*Takeover
}

// TakeoverProofMsg is the protocol msg sent by downstream peer
type TakeoverProofMsg TakeoverProof

// String pretty prints TakeoverProofMsg
func (m TakeoverProofMsg) String() string {
	return fmt.Sprintf("Stream: '%v' [%v-%v], Root: %x, Sig: %x", m.Stream, m.Start, m.End, m.Root, m.Sig)
}

func (p *Peer) handleTakeoverProofMsg(ctx context.Context, req *TakeoverProofMsg) error {
	_, err := p.getServer(req.Stream)
	// store the strongest takeoverproof for the stream in streamer
	return err
}

//收据消息，客户端从服务端收到检索的回应数据后，通过此格式向服务端发送签名
//如果客户端总是向服务端提交一个新签名，即STime为新的，AMount为0，那么服务端就可以断开该客户端的连接，并将该节点加入黑名单
//签名的收据总是用最低优先级发送，并且如果有新的可覆盖签名出现时，使用新的签名，可以直接丢弃老的签名
type ReceiptsMsg struct {
	PA     [20]byte
	STime  uint32
	AMount uint32
	Sig    []byte
}
