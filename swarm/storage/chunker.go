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
package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/hashicorp/golang-lru"
	"github.com/gauss-project/eswarm/common"
	"github.com/gauss-project/eswarm/rlp"
	"github.com/gauss-project/eswarm/swarm/util"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	olog "github.com/opentracing/opentracing-go/log"
	"github.com/gauss-project/eswarm/metrics"
	"github.com/gauss-project/eswarm/swarm/chunk"
	"github.com/gauss-project/eswarm/swarm/log"
	"github.com/gauss-project/eswarm/swarm/spancontext"
	rawHttp "net/http"
)

/*
The distributed storage implemented in this package requires fix sized chunks of content.

Chunker is the interface to a component that is responsible for disassembling and assembling larger data.

TreeChunker implements a Chunker based on a tree structure defined as follows:

1 each node in the tree including the root and other branching nodes are stored as a chunk.

2 branching nodes encode data contents that includes the size of the dataslice covered by its entire subtree under the node as well as the hash keys of all its children :
data_{i} := size(subtree_{i}) || key_{j} || key_{j+1} .... || key_{j+n-1}

3 Leaf nodes encode an actual subslice of the input data.

4 if data size is not more than maximum chunksize, the data is stored in a single chunk
  key = hash(int64(size) + data)

5 if data size is more than chunksize*branchesOfChunk^l, but no more than chunksize*
  branchesOfChunk^(l+1), the data vector is split into slices of chunksize*
  branchesOfChunk^l length (except the last one).
  key = hash(int64(size) + key(slice0) + key(slice1) + ...)

 The underlying hash function is configurable
*/

/*
Tree chunker is a concrete implementation of data chunking.
This chunker works in a simple way, it builds a tree out of the document so that each node either represents a chunk of real data or a chunk of data representing an branching non-leaf node of the tree. In particular each such non-leaf chunk will represent is a concatenation of the hash of its respective children. This scheme simultaneously guarantees data integrity as well as self addressing. Abstract nodes are transparent since their represented size component is strictly greater than their maximum data size, since they encode a subtree.

If all is well it is possible to implement this by simply composing readers so that no extra allocation or buffering is necessary for the data splitting and joining. This means that in principle there can be direct IO between : memory, file system, network socket (bzz peers storage request is read from the socket). In practice there may be need for several stages of internal buffering.
The hashing itself does use extra copies and allocation though, since it does need it.
*/

const (
	MAX_FILE_CACHE=26214500 //256*1024*100+100
)
type ChunkerParams struct {
	chunkSize int64
	hashSize  int64
}

type SplitterParams struct {
	ChunkerParams
	reader io.Reader
	putter Putter
	addr   Address
}

type TreeSplitterParams struct {
	SplitterParams
	size int64
}

type JoinerParams struct {
	ChunkerParams
	addr   Address
	getter Getter
	// TODO: there is a bug, so depth can only be 0 today, see: https://github.com/ethersphere/go-ethereum/issues/344
	depth int
	ctx   context.Context
}

type TreeChunker struct {
	ctx context.Context

	branchesOfChunk int64
	dataSize int64
	data     io.Reader
	// calculated
	addr        Address
	depth       int
	hashSize    int64        // self.hashFunc.New().Size()
	chunkSize   int64        // hashSize* branches
	workerCount int64        // the number of worker routines used
	workerLock  sync.RWMutex // lock for the worker count
	jobC        chan *hashJob
	wg          *sync.WaitGroup
	putter      Putter
	getter      Getter
	errC        chan error
	quitC       chan bool
}

/*
	Join reconstructs original content based on a root key.
	When joining, the caller gets returned a Lazy SectionReader, which is
	seekable and implements on-demand fetching of chunks as and where it is read.
	New chunks to retrieve are coming from the getter, which the caller provides.
	If an error is encountered during joining, it appears as a reader error.
	The SectionReader.
	As a result, partial reads from a document are possible even if other parts
	are corrupt or lost.
	The chunks are not meant to be validated by the chunker when joining. This
	is because it is left to the DPA to decide which sources are trusted.
*/
func TreeJoin(ctx context.Context, addr Address, getter Getter, depth int) *LazyChunkReader {
	jp := &JoinerParams{
		ChunkerParams: ChunkerParams{
			chunkSize: chunk.DefaultSize,
			hashSize:  int64(len(addr)),
		},
		addr:   addr,
		getter: getter,
		depth:  depth,
		ctx:    ctx,
	}

	return NewTreeJoiner(jp).Join(ctx)
}

/*
	When splitting, data is given as a SectionReader, and the key is a hashSize long byte slice (Key), the root hash of the entire content will fill this once processing finishes.
	New chunks to store are store using the putter which the caller provides.
*/
func TreeSplit(ctx context.Context, data io.Reader, size int64, putter Putter) (k Address, wait func(context.Context) error, err error) {
	tsp := &TreeSplitterParams{
		SplitterParams: SplitterParams{
			ChunkerParams: ChunkerParams{
				chunkSize: chunk.DefaultSize,
				hashSize:  putter.RefSize(),
			},
			reader: data,
			putter: putter,
		},
		size: size,
	}
	return NewTreeSplitter(tsp).Split(ctx)
}
//
func NewTreeJoiner(params *JoinerParams) *TreeChunker {
	tc := &TreeChunker{}
	tc.hashSize = params.hashSize     //每个哈希的大小（32字节）
	tc.branchesOfChunk = params.chunkSize / params.hashSize //每个区块可以容纳多少个分支，分支用哈希值表示，因此分支=区块/哈希大小
	tc.addr = params.addr			//块的地址
	tc.getter = params.getter		//数据读取器
	tc.depth = params.depth			//这个为什么需要预设未知
	tc.chunkSize = params.chunkSize	//每个区块的大小
	tc.workerCount = 0
	tc.jobC = make(chan *hashJob, 2*ChunkProcessors)
	tc.wg = &sync.WaitGroup{}
	tc.errC = make(chan error)
	tc.quitC = make(chan bool)

	tc.ctx = params.ctx

	return tc
}

func NewTreeSplitter(params *TreeSplitterParams) *TreeChunker {
	tc := &TreeChunker{}
	tc.data = params.reader
	tc.dataSize = params.size
	tc.hashSize = params.hashSize
	tc.branchesOfChunk = params.chunkSize / params.hashSize
	tc.addr = params.addr
	tc.chunkSize = params.chunkSize
	tc.putter = params.putter
	tc.workerCount = 0
	tc.jobC = make(chan *hashJob, 2*ChunkProcessors)
	tc.wg = &sync.WaitGroup{}
	tc.errC = make(chan error)
	tc.quitC = make(chan bool)

	return tc
}

type hashJob struct {
	key      Address
	chunk    []byte
	size     int64
	parentWg *sync.WaitGroup
}

func (tc *TreeChunker) incrementWorkerCount() {
	tc.workerLock.Lock()
	defer tc.workerLock.Unlock()
	tc.workerCount += 1
}

func (tc *TreeChunker) getWorkerCount() int64 {
	tc.workerLock.RLock()
	defer tc.workerLock.RUnlock()
	return tc.workerCount
}

func (tc *TreeChunker) decrementWorkerCount() {
	tc.workerLock.Lock()
	defer tc.workerLock.Unlock()
	tc.workerCount -= 1
}

func (tc *TreeChunker) Split(ctx context.Context) (k Address, wait func(context.Context) error, err error) {
	if tc.chunkSize <= 0 {
		panic("chunker must be initialised")
	}

	tc.runWorker(ctx)

	depth := 0
	treeSize := tc.chunkSize

	// takes lowest depth such that chunksize*HashCount^(depth+1) > size
	// power series, will find the order of magnitude of the data size in base hashCount or numbers of levels of branching in the resulting tree.
	for ; treeSize < tc.dataSize; treeSize *= tc.branchesOfChunk {
		depth++
	}

	key := make([]byte, tc.hashSize)
	// this waitgroup member is released after the root hash is calculated
	tc.wg.Add(1)
	//launch actual recursive function passing the waitgroups
	go tc.split(ctx, depth, treeSize/tc.branchesOfChunk, key, tc.dataSize, tc.wg)

	// closes internal error channel if all subprocesses in the workgroup finished
	go func() {
		// waiting for all threads to finish
		tc.wg.Wait()
		close(tc.errC)
	}()

	defer close(tc.quitC)
	defer tc.putter.Close()
	select {
	case err := <-tc.errC:
		if err != nil {
			return nil, nil, err
		}
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	return key, tc.putter.Wait, nil
}

func (tc *TreeChunker) split(ctx context.Context, depth int, treeSize int64, addr Address, size int64, parentWg *sync.WaitGroup) {

	//

	for depth > 0 && size < treeSize {
		treeSize /= tc.branchesOfChunk
		depth--
	}

	if depth == 0 {
		// leaf nodes -> content chunks
		chunkData := make([]byte, size+8)
		binary.LittleEndian.PutUint64(chunkData[0:8], uint64(size))
		var readBytes int64
		for readBytes < size {
			n, err := tc.data.Read(chunkData[8+readBytes:])
			readBytes += int64(n)
			if err != nil && !(err == io.EOF && readBytes == size) {
				tc.errC <- err
				return
			}
		}
		select {
		case tc.jobC <- &hashJob{addr, chunkData, size, parentWg}:
		case <-tc.quitC:
		}
		return
	}
	// dept > 0
	// intermediate chunk containing child nodes hashes
	branchCnt := (size + treeSize - 1) / treeSize

	var chunk = make([]byte, branchCnt*tc.hashSize+8)
	var pos, i int64

	binary.LittleEndian.PutUint64(chunk[0:8], uint64(size))

	childrenWg := &sync.WaitGroup{}
	var secSize int64
	for i < branchCnt {
		// the last item can have shorter data
		if size-pos < treeSize {
			secSize = size - pos
		} else {
			secSize = treeSize
		}
		// the hash of that data
		subTreeAddress := chunk[8+i*tc.hashSize : 8+(i+1)*tc.hashSize]

		childrenWg.Add(1)
		tc.split(ctx, depth-1, treeSize/tc.branchesOfChunk, subTreeAddress, secSize, childrenWg)

		i++
		pos += treeSize
	}
	// wait for all the children to complete calculating their hashes and copying them onto sections of the chunk
	// parentWg.Add(1)
	// go func() {
	childrenWg.Wait()

	worker := tc.getWorkerCount()
	if int64(len(tc.jobC)) > worker && worker < ChunkProcessors {
		tc.runWorker(ctx)

	}
	select {
	case tc.jobC <- &hashJob{addr, chunk, size, parentWg}:
	case <-tc.quitC:
	}
}

func (tc *TreeChunker) runWorker(ctx context.Context) {
	tc.incrementWorkerCount()
	go func() {
		defer tc.decrementWorkerCount()
		for {
			select {

			case job, ok := <-tc.jobC:
				if !ok {
					return
				}

				h, err := tc.putter.Put(ctx, job.chunk)
				if err != nil {
					tc.errC <- err
					return
				}
				copy(job.key, h)
				job.parentWg.Done()
			case <-tc.quitC:
				return
			}
		}
	}()
}

type RpData struct {
	Stime int64

	Amount int64
}

type ReportData map[time.Time]int64

func (r *ReportData) EncodeRLP(w io.Writer) error {
	value := make([]*RpData, 0)
	for stime, amount := range *r {
		data := RpData{stime.UnixNano(), amount}
		value = append(value, &data)
	}
	return rlp.Encode(w, &value)
}
func (rd *ReportData) DecodeRLP(s *rlp.Stream) error {
	value := make([]*RpData, 0)
	if err := s.Decode(&value); err != nil {
		return err
	}
	for _, res := range value {
		(*rd)[time.Unix(0, res.Stime)] = res.Amount
	}

	return nil
}

// LazyChunkReader implements LazySectionReader
type LazyChunkReader struct {
	ctx       context.Context
	addr      Address // root address
	chunkData ChunkData  //这个是addr对应的根数据，（8字节长度)+chunk哈希值或者chunk数据
	off       int64 // offset
	chunkSize int64 // inherit from chunker
	branchesOfChunk  int64 // inherit from chunker
	hashSize  int64 // inherit from chunker
	depth     int
	getter    Getter
	sizeCache *lru.Cache
	ts_buffer *lru.Cache //这个从中心化服务器预读取的片断
}

func (tc *TreeChunker) Join(ctx context.Context) *LazyChunkReader {
	bf, _ := lru.New(50)
	sizeCache, err := lru.New(1000)
	if err != nil {
		fmt.Println("Error of create lru cache,reaseon", err.Error())
	}
	return &LazyChunkReader{
		addr:      tc.addr,
		chunkSize: tc.chunkSize,
		branchesOfChunk:  tc.branchesOfChunk,
		hashSize:  tc.hashSize,
		depth:     tc.depth,
		getter:    tc.getter,
		ctx:       tc.ctx,
		sizeCache: sizeCache,
		ts_buffer: bf,
	}
}

func (r *LazyChunkReader) Context() context.Context {
	return r.ctx
}

type int64str struct {
	value  int32
	hvalue int32
}

// Size is meant to be called on the LazySectionReader
func (r *LazyChunkReader) Size(ctx context.Context, quitC chan bool) (n int64, err error) {
	metrics.GetOrRegisterCounter("lazychunkreader.size", nil).Inc(1)

	var sp opentracing.Span
	var cctx context.Context

	cctx, sp = spancontext.StartSpan(
		ctx,
		"lcr.size")
	defer sp.Finish()
	sizeValue, ok := r.sizeCache.Get(r.addr.Hex())
	if ok {
		//value := sizeValue.(int64str)
		return sizeValue.(int64), nil
	}

	//log.Debug("lazychunkreader.size", "addr", r.addr)
	if r.chunkData == nil {
		startTime := time.Now()
		chunkData, err := r.getter.Get(cctx, Reference(r.addr))
		if err != nil {
			metrics.GetOrRegisterResettingTimer("lcr.getter.get.err", nil).UpdateSince(startTime)
			return 0, err
		}
		metrics.GetOrRegisterResettingTimer("lcr.getter.get", nil).UpdateSince(startTime)
		r.chunkData = chunkData
	}

	s := r.chunkData.Size()
	log.Debug("lazychunkreader.size", "key", r.addr, "size", s)
	//val := int64str{int32(s),int32(s>>32)}
	r.sizeCache.Add(r.addr.Hex(), int64(s))
	return int64(s), nil
}

type DataCache struct {
	start int64
	value []byte
	end   bool
}

// read at can be called numerous times
// concurrent reads are allowed
// Size() needs to be called synchronously on the LazyChunkReader first
func (r *LazyChunkReader) ReadAt(b []byte, off int64) (read int, err error) {

	metrics.GetOrRegisterCounter("lazychunkreader.readat", nil).Inc(1)

	var sp opentracing.Span
	var cctx context.Context
	cctx, sp = spancontext.StartSpan(
		r.ctx,
		"lcr.read")
	defer sp.Finish()

	defer func() {
		sp.LogFields(
			olog.Int("off", int(off)),
			olog.Int("read", read))
	}()

	// this is correct, a swarm doc cannot be zero length, so no EOF is expected
	if len(b) == 0 {
		return 0, nil
	}


	defer func() { // 必须要先声明defer，否则不能捕获到panic异常

		if err := recover(); err != nil {
			log.Error("Error recovered!", "err", err) // 这里的err其实就是panic传入的内容，55
			read = 0
			err = errors.New("Ooops, request is nil!")
		}

	}()

	requestInfo := cctx.Value("request")
	var url string
	var req *rawHttp.Request
	if requestInfo != nil && !reflect.ValueOf(requestInfo).IsNil() {
		req = requestInfo.(*rawHttp.Request)
		url = cctx.Value("url").(string)
	}else {
		url = r.addr.String()
	}
		buffer, OK := r.ts_buffer.Get(url)
		needRetrieve := !OK
		//检查是否需要
		if OK {
			result := buffer.(*DataCache)
			if off < result.start  || //不在缓冲之内
				(!result.end && (off+int64(len(b)) > (result.start + util.MaxLen))) {
				needRetrieve = true

			}
		}

		startOffset := int64(0)
		if needRetrieve {


			//////------- first read from P2P network--------/////
			quitC := make(chan bool)
			size, err2 := r.Size(cctx, quitC)
			if err2 == nil {

				errC := make(chan error)

				/**
					通过size获得tree应该是几层
				 */
				var treeSize int64
				var depth int
				// calculate depth and max treeSize
				treeSize = r.chunkSize
				//指数级增长，这个方案避免了使用log函数
				for ; treeSize < size; treeSize *= r.branchesOfChunk {
					depth++
				}

				max_read := size-off;
				fileEnd := false
				if max_read > util.MaxLen {//256*1024*100+100
					max_read = util.MaxLen
				}else{
					fileEnd = true
				}
				data_buffer := make([]byte,max_read)
				length := int64(len(data_buffer))
				for d := 0; d < r.depth; d++ {
					off *= r.chunkSize
					length *= r.chunkSize
				}

				go r.join(cctx, data_buffer, off, off+length, depth, treeSize/r.branchesOfChunk, r.chunkData, errC)

				err = <-errC
				if err == nil { //replace cache and return data from cache

					buffer = &DataCache{start: off, value: data_buffer, end: fileEnd}
					r.ts_buffer.Add(url, buffer)
					//}

				}
			}else if req !=nil {
				/////------------fallback read from center server --------///////////////
				httpClient := cctx.Value("reporter")
				if httpClient != nil && !reflect.ValueOf(httpClient).IsNil() {
					hash := cctx.Value("hash")
					var hashValue common.Hash
					if hash != nil && reflect.ValueOf(hash).IsValid() {
						hashValue = hash.(common.Hash)
					} else {
						hashValue = common.Hash{0}
					}
					//	fmt.Printf("Read hash from central node: %v len:%v from: %v\r\n",url,len(b),off)
					dataBuf, end := httpClient.(*util.HttpReader).GetChunkFromCentral(url, off, hashValue[:], req)
					if dataBuf != nil && len(dataBuf) > 0 {

						buffer = DataCache{start: off, value: dataBuf, end: end}
						r.ts_buffer.Add(url, buffer)
						err = nil
					} else {
						buffer = nil
						err = errors.New("Read Central Failed")
					}
					startOffset = 0
				} else {
					buffer = nil
					err = errors.New("No Central Found")
				}
			}

		} else {

			startOffset = off - buffer.(*DataCache).start
			err = nil

		}
		if buffer != nil {
			cacheBuffer := buffer.(*DataCache).value
			totalLen := len(cacheBuffer) - int(startOffset)
			if totalLen > len(b) {
				totalLen = len(b)

			} else if totalLen < 0 {
				totalLen = 0
			}
			//测试是否文件已经读完的两个条件：
			// 1. buffer已经指向了addr/url对应的文件尾
			// 2. b的缓冲区长度加上开始的位置已经超过buffer里面末尾的大小了
			if buffer.(*DataCache).end  && (int(startOffset) + len(b) >= len(cacheBuffer)){
				err=io.EOF
			}
			copy(b, cacheBuffer[startOffset:startOffset+int64(totalLen)])
			read = totalLen
		}else{
			read = 0;

		}

		return



}
/**
	获取从off开始到eoff中一段数据，放置到b中，数据根是chunkData,chunksAccount是这个r的实际数据的chunks个数
	off/eoff是原始的 offset和length映射到depth层的的结果

 */
func (r *LazyChunkReader) join(ctx context.Context, b []byte, off int64, eoff int64, depth int, chunksAccount int64, chunkData ChunkData, errC chan error) {
	result := error(nil)

	defer func() {
		//把错误信息提交给上一层
		errC <- result
	}()
	// find appropriate block level
	for chunkData.Size() < uint64(chunksAccount) && depth > r.depth {
		chunksAccount /= r.branchesOfChunk
		depth--
	}
	//sec := time.Now().UnixNano()
	//log.Info("Join Start","uuid",sec,"offset",off,"eoff",eoff,"depth",depth,"addr",chunkData[0:20])

	//defer func() {log.Info("Join end","uuid",sec,"offset",off,"eoff",eoff,"depth",depth,"addr",chunkData[0:20])}()
	// leaf chunk found，最后一层，叶子节点
	if depth == r.depth {
		extra := 8 + eoff - int64(len(chunkData))
		if extra > 0 {
			eoff -= extra
		}
		copy(b, chunkData[8+off:8+eoff])
		return // simply give back the chunks reader for content chunks
	}

	// subtree
	start := off / chunksAccount
	end := (eoff + chunksAccount - 1) / chunksAccount

	// last non-leaf chunk can be shorter than default chunk size, let's not read it further then its end
	currentBranches := int64(len(chunkData)-8) / r.hashSize
	if end > currentBranches {
		end = currentBranches
	}

	errs := make(chan error, end-start)
	for i := start; i < end; i++ {
		soff := i * chunksAccount
		roff := soff
		seoff := soff + chunksAccount

		if soff < off {
			soff = off
		}
		if seoff > eoff {
			seoff = eoff
		}

		go func(j int64) {
			childAddress := chunkData[8+j*r.hashSize : 8+(j+1)*r.hashSize]
			startTime := time.Now()
			chunkData, err := r.getter.Get(ctx, Reference(childAddress))
			if err != nil {
				metrics.GetOrRegisterResettingTimer("lcr.getter.get.err", nil).UpdateSince(startTime)
				log.Debug("lazychunkreader.join", "key", fmt.Sprintf("%x", childAddress), "err", err)
				select {
				case errs <- fmt.Errorf("chunk %v-%v not found; key: %s", off, off+chunksAccount, fmt.Sprintf("%x", childAddress)):
				case <-ctx.Done():
					errs <- errors.New("quited")
				}
				return
			}
			metrics.GetOrRegisterResettingTimer("lcr.getter.get", nil).UpdateSince(startTime)
			if l := len(chunkData); l < 9 {
				select {
				case errs <- fmt.Errorf("chunk %v-%v incomplete; key: %s, data length %v", off, off+chunksAccount, fmt.Sprintf("%x", childAddress), l):
				case <-ctx.Done():
					errs <- errors.New("quited")
				}
				return
			}
			if soff < off {
				soff = off
			}
			r.join(ctx, b[soff-off:seoff-off], soff-roff, seoff-roff, depth-1, chunksAccount/r.branchesOfChunk, chunkData, errs)
		}(i)
	} //for

	//查看所有的errs，如果有错误，返回给上一层,这一段具有阻塞作用，只有等所有的子线程都返回后，才会返回

	for i := 0; i < int(end-start); i++ {
		select {
		case res := <-errs:
			if res != nil {
				result = res
			}
		}
	}

}

// Read keeps a cursor so cannot be called simulateously, see ReadAt
func (r *LazyChunkReader) Read(b []byte) (read int, err error) {
	//log.Debug("lazychunkreader.read", "key", r.addr)
	metrics.GetOrRegisterCounter("lazychunkreader.read", nil).Inc(1)

	read, err = r.ReadAt(b, r.off)
	if err != nil && err != io.EOF {
		log.Debug("lazychunkreader.readat", "read", read, "err", err)
		metrics.GetOrRegisterCounter("lazychunkreader.read.err", nil).Inc(1)
	}

	metrics.GetOrRegisterCounter("lazychunkreader.read.bytes", nil).Inc(int64(read))

	r.off += int64(read)
	if read == 0 && err == nil {
		log.Error("0BYTES READ!", "read", read, "err", err)
		err = errors.New("ZERO BYTES READ")
	}
	return read, err
}

// completely analogous to standard SectionReader implementation
var errWhence = errors.New("Seek: invalid whence")
var errOffset = errors.New("Seek: invalid offset")

func (r *LazyChunkReader) Seek(offset int64, whence int) (int64, error) {
	cctx, sp := spancontext.StartSpan(
		r.ctx,
		"lcr.seek")
	defer sp.Finish()

	log.Debug("lazychunkreader.seek", "key", r.addr, "offset", offset)
	switch whence {
	default:
		return 0, errWhence
	case 0:
		offset += 0
	case 1:
		offset += r.off
	case 2:

		if r.chunkData == nil { //seek from the end requires rootchunk for size. call Size first
			_, err := r.Size(cctx, nil)
			if err != nil {
				return 0, fmt.Errorf("can't get size: %v", err)
			}
		}
		offset += int64(r.chunkData.Size())
	}

	if offset < 0 {
		return 0, errOffset
	}
	r.off = offset
	return offset, nil
}
