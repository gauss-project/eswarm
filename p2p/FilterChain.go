package p2p

import (
	"github.com/gauss-project/eswarm/p2p/enode"
)

type FiterItem interface {
	IsBlocked(id enode.ID, inbound bool) bool
	CanSendPing(id enode.ID) bool
	CanProcPing(id enode.ID) bool
	CanProcPong(id enode.ID) bool
	CanStartConnect(id enode.ID) bool
	ShouldAcceptConn(id enode.ID) bool
}

type FilterChain struct {
	Filters []FiterItem
}

func NewFilterChain() *FilterChain {
	return &FilterChain{
		Filters: make([]FiterItem, 0),
	}
}

func (fc *FilterChain) AddFilter(ft FiterItem) {
	fc.Filters = append(fc.Filters, ft)
}

func (fc *FilterChain) IsBlocked(id enode.ID, inbound bool) bool {
	for _, ft := range fc.Filters {
		if ft.IsBlocked(id, inbound) {
			//			log.Info(" blocked peer:","id",id,"inbound",inbound)
			return true
		}
	}
	return false
}
func (fc *FilterChain) CanSendPing(id enode.ID) bool {
	if fc.IsBlocked(id, false) {
		return false
	}
	for _, ft := range fc.Filters {
		if !ft.CanSendPing(id) {
			return false
		}
	}
	return true
}
func (fc *FilterChain) CanProcPing(id enode.ID) bool {
	if fc.IsBlocked(id, false) {
		return false
	}
	for _, ft := range fc.Filters {
		if !ft.CanProcPing(id) {
			return false
		}
	}
	return true
}
func (fc *FilterChain) CanProcPong(id enode.ID) bool {
	if fc.IsBlocked(id, false) {
		return false
	}
	for _, ft := range fc.Filters {
		if ft.CanProcPong(id) {
			return false
		}
	}
	return true
}
func (fc *FilterChain) CanStartConnect(id enode.ID) bool {
	if fc.IsBlocked(id, false) {
		return false
	}
	for _, ft := range fc.Filters {
		if !ft.CanStartConnect(id) {
			return false
		}
	}
	return true
}
func (fc *FilterChain) ShouldAcceptConn(id enode.ID) bool {
	if fc.IsBlocked(id, false) {
		return false
	}
	for _, ft := range fc.Filters {
		if !ft.ShouldAcceptConn(id) {
			return false
		}
	}
	return true
}
