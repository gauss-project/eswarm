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
	"github.com/gauss-project/eswarm/log"
	"path/filepath"
	"sync"

	"github.com/gauss-project/eswarm/metrics"
	"github.com/gauss-project/eswarm/swarm/storage/mock"
)

type LocalStoreParams struct {
	*StoreParams
	ChunkInfoPath string
	ChunkDbPath string
	Validators  []ChunkValidator `toml:"-"`
}

func NewDefaultLocalStoreParams() *LocalStoreParams {
	return &LocalStoreParams{
		StoreParams: NewDefaultStoreParams(),
	}
}

//this can only finally be set after all config options (file, cmd line, env vars)
//have been evaluated
func (p *LocalStoreParams) Init(path string) {
	p.ChunkInfoPath = path
	if p.ChunkDbPath == "" {
		p.ChunkDbPath = filepath.Join(path, "chunks")

	}else {
		p.ChunkInfoPath = filepath.Join(path, "infos")
	}
}

// LocalStore is a combination of inmemory db over a disk persisted db
// implements a Get/Put with fallback (caching) logic using any 2 ChunkStores
type LocalStore struct {
	Validators []ChunkValidator
	memStore   *MemStore
	DbStore    *LDBStore
	mu         sync.Mutex
}

// This constructor uses MemStore and DbStore as components
func NewLocalStore(params *LocalStoreParams, mockStore *mock.NodeStore) (*LocalStore, error) {
	ldbparams := NewLDBStoreParams(params.StoreParams, params.ChunkDbPath,params.ChunkInfoPath)
	dbStore, err := NewMockDbStore(ldbparams, mockStore)
	if err != nil {
		return nil, err
	}
	return &LocalStore{
		memStore:   NewMemStore(params.StoreParams, dbStore),
		DbStore:    dbStore,
		Validators: params.Validators,
	}, nil
}

func NewTestLocalStoreForAddr(params *LocalStoreParams) (*LocalStore, error) {
	ldbparams := NewLDBStoreParams(params.StoreParams, params.ChunkDbPath,params.ChunkInfoPath)
	dbStore, err := NewLDBStore(ldbparams)
	if err != nil {
		return nil, err
	}
	localStore := &LocalStore{
		memStore:   NewMemStore(params.StoreParams, dbStore),
		DbStore:    dbStore,
		Validators: params.Validators,
	}
	return localStore, nil
}
func (ls *LocalStore) Validate(ch []byte, addr Address) bool {

	return ls.isValid(NewChunk(addr, ch))
}

// isValid returns true if chunk passes any of the LocalStore Validators.
// isValid also returns true if LocalStore has no Validators.
func (ls *LocalStore) isValid(chunk Chunk) bool {
	// by default chunks are valid. if we have 0 validators, then all chunks are valid.
	valid := true

	// ls.Validators contains a list of one validator per chunk type.
	// if one validator succeeds, then the chunk is valid
	for _, v := range ls.Validators {
		if valid = v.Validate(chunk); valid {
			break
		}
	}
	return valid
}

// Put is responsible for doing validation and storage of the chunk
// by using configured ChunkValidators, MemStore and LDBStore.
// If the chunk is not valid, its GetErrored function will
// return ErrChunkInvalid.
// This method will check if the chunk is already in the MemStore
// and it will return it if it is. If there is an error from
// the MemStore.Get, it will be returned by calling GetErrored
// on the chunk.
// This method is responsible for closing Chunk.ReqC channel
// when the chunk is stored in memstore.
// After the LDBStore.Put, it is ensured that the MemStore
// contains the chunk with the same data, but nil ReqC channel.
func (ls *LocalStore) Put(ctx context.Context, chunk Chunk) error {
	if !ls.isValid(chunk) {
		return ErrChunkInvalid
	}

	//log.Trace("localstore.put", "key", chunk.Address())
	ls.mu.Lock()
	defer ls.mu.Unlock()

	_, err := ls.memStore.Get(ctx, chunk.Address())
	if err == nil {
		return nil
	}
	if err != nil && err != ErrChunkNotFound {
		return err
	}
	ls.memStore.Put(ctx, chunk)
	if ls.DbStore != nil {
		err = ls.DbStore.Put(ctx, chunk)
	}else{
		err = nil
	}

	return err
}

// Has queries the underlying DbStore if a chunk with the given address
// is being stored there.
// Returns true if it is stored, false if not
func (ls *LocalStore) Has(ctx context.Context, addr Address) bool {
	if ls.DbStore != nil {
		return ls.DbStore.Has(ctx, addr)
	}else{
		return ls.memStore.Has(ctx,addr)
	}

}

// Get(chunk *Chunk) looks up a chunk in the local stores
// This method is blocking until the chunk is retrieved
// so additional timeout may be needed to wrap this call if
// ChunkStores are remote and can have long latency
func (ls *LocalStore) Get(ctx context.Context, addr Address) (chunk Chunk, err error) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	return ls.get(ctx, addr)
}

func (ls *LocalStore) get(ctx context.Context, addr Address) (chunk Chunk, err error) {
	chunk, err = ls.memStore.Get(ctx, addr)

	if err != nil && err != ErrChunkNotFound {
		metrics.GetOrRegisterCounter("localstore.get.error", nil).Inc(1)
		return nil, err
	}



	metrics.GetOrRegisterCounter("localstore.get.cachemiss", nil).Inc(1)
	if ls.DbStore != nil {

		if err == nil {
			metrics.GetOrRegisterCounter("localstore.get.cachehit", nil).Inc(1)
			go ls.DbStore.MarkAccessed(addr)
			return chunk, nil
		}
		chunk, err = ls.DbStore.Get(ctx, addr)
		if err != nil {
			metrics.GetOrRegisterCounter("localstore.get.error", nil).Inc(1)
			return nil, err
		}

		ls.memStore.Put(ctx, chunk)
		return chunk, nil
	}else{
		return nil, ErrChunkNotFound
	}

}

//从本地获取一个数据片断，如果没有，返回一个函数，该函数使用context作为参数
//在需要的时候，这个返回的函数可以放到goRoutine里，
func (ls *LocalStore) FetchFunc(ctx context.Context, addr Address) func(context.Context) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	_, err := ls.get(ctx, addr)
	if err == nil {
		return nil
	}
	return func(context.Context) error {
		return err
	}
}

func (ls *LocalStore) BinIndex(po uint8) uint64 {
	if ls.DbStore != nil {
		return ls.DbStore.BinIndex(po)
	}else{
		return 0
	}

}

func (ls *LocalStore) Iterator(from uint64, to uint64, po uint8, f func(Address, uint64) bool) error {
	if ls.DbStore != nil {
		return ls.DbStore.SyncIterator(from, to, po, f)
	}else{
		return nil
	}

}

// Close the local store
func (ls *LocalStore) Close() {
	if ls.DbStore != nil  {
		ls.DbStore.Close()
	}

}

// Migrate checks the datastore schema vs the runtime schema and runs
// migrations if they don't match
func (ls *LocalStore) Migrate() error {
	if ls.DbStore != nil {
		actualDbSchema, err := ls.DbStore.GetSchema()
		if err != nil {
			log.Error(err.Error())
			return err
		}

		if actualDbSchema == CurrentDbSchema {
			return nil
		}

		log.Debug("running migrations for", "schema", actualDbSchema, "runtime-schema", CurrentDbSchema)

		if actualDbSchema == DbSchemaNone {
			ls.migrateFromNoneToPurity()
			actualDbSchema = DbSchemaPurity
		}

		if err := ls.DbStore.PutSchema(actualDbSchema); err != nil {
			return err
		}

		if actualDbSchema == DbSchemaPurity {
			if err := ls.migrateFromPurityToHalloween(); err != nil {
				return err
			}
			actualDbSchema = DbSchemaHalloween
		}

		if err := ls.DbStore.PutSchema(actualDbSchema); err != nil {
			return err
		}
	}

	return nil
}

func (ls *LocalStore) migrateFromNoneToPurity() {
	// delete chunks that are not valid, i.e. chunks that do not pass
	// any of the ls.Validators
	if ls.DbStore !=nil  {
		ls.DbStore.Cleanup(func(c Chunk) bool {
			return !ls.isValid(c)
		})
	}

}

func (ls *LocalStore) migrateFromPurityToHalloween() error {
	if ls.DbStore !=nil {
		return ls.DbStore.CleanGCIndex()
	}
	return nil
}
