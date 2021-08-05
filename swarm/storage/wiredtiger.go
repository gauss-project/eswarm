// +build !android,!ios

package storage

import (
	"fmt"
	"github.com/gauss-project/eswarm/log"
	"github.com/gauss-project/eswarm/swarm/chunk"
	"github.com/plotozhu/wiredtiger-go/wiredtiger"
	"math"
	"os"
	"sync"
)
type TYPE_ENUM  uint8
const (
	T_QUERY TYPE_ENUM= 0  //查询
	T_UPDATE TYPE_ENUM= 1 //更新
	T_DELETE TYPE_ENUM=2  //删除
)
type requestItem struct {
	_type TYPE_ENUM
	address chunk.Address
	data    []byte
	ret     chan *resultItem
}

type resultItem struct {
	err   error
	data    []byte
}

type oneShard struct {
	readSession *wiredtiger.Session
	session *wiredtiger.Session
	rdCursor *wiredtiger.Cursor
	cursor  *wiredtiger.Cursor
	rdInputChan   chan *requestItem
	inputChan   chan *requestItem
	lock    sync.Mutex
}
type WiredtigerDB struct {
	path string
	shardMasks int
	shardsCount int
	conn  *wiredtiger.Connection
	shardItems []*oneShard
	quitC  chan struct{}
}
func NewDatabase(filePath string,shardOverlay int) *WiredtigerDB {
	shardCount := int(math.Pow(2,float64(shardOverlay)))
	db := WiredtigerDB{
		path:filePath,
		shardsCount:shardCount,
		shardMasks:int(shardCount-1),
		shardItems:make([]*oneShard,shardCount),
	}

	db.OpenDB();

	return &db
}

func (db *WiredtigerDB)OpenDB(){
	db.openDB()
	//db.Close()
	//db.openDB()
	db.start()
}
func (db *WiredtigerDB)openDB(){
	if db.conn != nil {
		db.conn.Close("")
	}

	var err error

	err  =os.MkdirAll(db.path, os.ModePerm)
	db.conn,err = wiredtiger.Open(db.path,"create,cache_size=256M,checkpoint=(log_size=20000000,wait=30),async=(enabled=true,threads=4)")

	if err != nil {
		log.Error(err.Error())
	}

	for i := 0; i < db.shardsCount;i++ {

		session, err := db.conn.OpenSession("")
		if err != nil {

			panic(fmt.Sprintf("Failed to create session: %v", err.Error()))
		}
		//session.Salvage(fmt.Sprintf("table:rawchunks%d",i),"")
		err = session.Create(fmt.Sprintf("table:rawchunks%d",i), "key_format=u,value_format=u")
		if err != nil {
			panic(fmt.Sprintf("Failed to open cursor table: %v", err.Error()))
		}
		cursor, err := session.OpenCursor(fmt.Sprintf("table:rawchunks%d",i), nil, "")

		rdSession, err := db.conn.OpenSession("")
		if err != nil {

			panic(fmt.Sprintf("Failed to create session: %v", err.Error()))
		}
		//session.Salvage(fmt.Sprintf("table:rawchunks%d",i),"")
		err = rdSession.Create(fmt.Sprintf("table:rawchunks%d",i), "key_format=u,value_format=u")
		if err != nil {
			panic(fmt.Sprintf("Failed to open cursor table: %v", err.Error()))
		}
		rdCursor, err := rdSession.OpenCursor(fmt.Sprintf("table:rawchunks%d",i), nil, "readonly=true")

		db.shardItems[i] = &oneShard{
			session:session,
			cursor:cursor,
			readSession:rdSession,
			rdCursor:rdCursor,
			inputChan:make(chan *requestItem),
			rdInputChan:make(chan *requestItem),
		}
	}
	db.quitC = make(chan struct{})

}

func (db *WiredtigerDB)start(){

	for i := 0; i < db.shardsCount; i++{
		go func (index int ){
			for {
				select {
					case item := <- db.shardItems[index].inputChan:
						if item != nil {
							db.procRequest(db.shardItems[index],item)
						}

					case <- db.quitC:
							return
				}
			}
		}(i)
		//read thread
		go func (index int ){
			for {
				select {
				case item := <- db.shardItems[index].rdInputChan:
					if item != nil {
						db.procRdRequest(db.shardItems[index],item)
					}

				case <- db.quitC:
					return
				}
			}
		}(i)
	}
}

func (db *WiredtigerDB) Close(){
	close(db.quitC)
	for i := 0; i < db.shardsCount; i++{
		if db.shardItems[i] != nil {
			db.shardItems[i].cursor.Close()
			db.shardItems[i].session.Close("")
			close(db.shardItems[i].inputChan)
		}
	}

	db.conn.Close("")
	db.conn = nil
}
func (db *WiredtigerDB)procRequest(shardItem *oneShard,request *requestItem){
	shardItem.lock.Lock()
	defer shardItem.lock.Unlock()
	switch request._type {
	case T_UPDATE:
		key := request.address

		err := shardItem.cursor.SetKey([]byte(key[:]))
		if err != nil {
			log.Error("error in set key", "reason", err)
		}
		err = shardItem.cursor.SetValue(encodeData(NewChunk(request.address,request.data)))
		if err != nil {
			log.Error("error in set data", "reason", err)

		}
		err = shardItem.cursor.Insert()
		if err != nil {
			log.Error("Failed to insert", "error", err)
		} else {

			//			log.Info("Ok to insert","addr",chunk.Address(),"value",chunk.Data()[:10])
		} //<- s.waitChan
		//	}()

		request.ret <- &resultItem{err,[]byte{}}
	case T_DELETE:
		shardItem.cursor.SetKey([]byte(request.address))
		err := shardItem.cursor.Remove()
		if err != nil {
			log.Error("Failed to delete", "error", err.Error())
		} else {
			log.Info("chunk deleted:", "addr", request.address)

		}

		request.ret <- &resultItem{err,[]byte{}}
	}
}

func (db *WiredtigerDB)procRdRequest(shardItem *oneShard,request *requestItem){
	switch request._type {
	case T_QUERY:
		value := make([]byte, 0)
		shardItem.cursor.SetKey([]byte(request.address[:]))
		err := shardItem.cursor.Search()
		if err == nil {
			err = shardItem.cursor.GetValue(&value)
		}
		if err != nil {
			log.Error("Failed to lookup", "addr", request.address, "error", err.Error())
		}

		request.ret <- &resultItem{err, value}
	}
}


// encodeDataFunc returns a function that stores the chunk data
// to a mock store to bypass the default functionality encodeData.
// The constructed function always returns the nil data, as DbStore does
// not need to store the data, but still need to create the index.
func (db *WiredtigerDB)NewWtEncodeDataFunc() func(chunk chunk.Chunk) ([]byte,error) {


	return func(chunk chunk.Chunk) ([]byte,error) {
		shardId := int(chunk.Address()[0]) & db.shardMasks
		shardItem := db.shardItems[shardId]
		result :=make(chan *resultItem)
		req := requestItem{T_UPDATE,chunk.Address(),chunk.Data(),result}
		shardItem.inputChan <- &req
		ret := <- result
		return chunk.Address()[:],ret.err
	}
}
type resultV struct {
	data []byte
	err error
}
func (db *WiredtigerDB)NewWtGetDataFunc() func(addr chunk.Address) (data []byte, err error) {


	return func(addr chunk.Address) (data []byte, err error) {

		shardId := int(addr[0]) & db.shardMasks
		shardItem := db.shardItems[shardId]
		result :=make(chan *resultItem)
		req := requestItem{T_QUERY,addr,[]byte{},result}
		shardItem.rdInputChan <- &req
		ret := <- result
		return ret.data,ret.err

	}
}

func (db *WiredtigerDB)NewWtDeleteDataFunc() func(addr chunk.Address) (err error) {
	//创建一个删除线程
	return func(addr chunk.Address) (err error) {
		shardId := int(addr[0]) & db.shardMasks
		shardItem := db.shardItems[shardId]
		result :=make(chan *resultItem)
		req := requestItem{T_DELETE,addr,[]byte{},result}
		shardItem.inputChan <- &req
		ret := <- result
		return ret.err
	}
}