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

// Contains all the wrappers from the accounts package to support client side enode
// management on mobile platforms.

package csdc

import (
	"errors"
	"github.com/gauss-project/eswarm/p2p/enode"
)

// NewEnode parses a node designator.
//
// There are two basic forms of node designators
//   - incomplete nodes, which only have the public key (node ID)
//   - complete nodes, which contain the public key and IP/Port information
//
// For incomplete nodes, the designator must look like one of these
//
//    enode://<hex node id>
//    <hex node id>
//
// For complete nodes, the node ID is encoded in the username portion
// of the URL, separated from the host by an @ sign. The hostname can
// only be given as an IP address, DNS domain names are not allowed.
// The port in the host name section is the TCP listening port. If the
// TCP and UDP (discovery) ports differ, the UDP port is specified as
// query parameter "discport".
//
// In the following example, the node URL describes
// a node with IP address 10.3.58.6, TCP listening port 30303
// and UDP discovery port 30301.
//
//    enode://<hex node id>@10.3.58.6:30303?discport=30301

// Enode represents a host on the network.
type Enodev4 struct {
	node *enode.Node
}

// Enodes represents a slice of accounts.
type Enodesv4 struct {
	nodes []*enode.Node
}

func NewEnodev4(rawurl string) (enodev4 *Enodev4, _ error) {
	node, err := enode.ParseV4(rawurl)
	if err != nil {
		return nil, err
	}
	return &Enodev4{node}, nil
}

// NewEnodes creates a slice of uninitialized enodes.
func NewEnodesv4(size int) *Enodesv4 {
	return &Enodesv4{
		nodes: make([]*enode.Node, size),
	}
}

// NewEnodesEmpty creates an empty slice of Enode values.
func NewEnodesEmptyv4() *Enodesv4 {
	return NewEnodesv4(0)
}

// Size returns the number of enodes in the slice.
func (e *Enodesv4) Size() int {
	return len(e.nodes)
}

// Get returns the enode at the given index from the slice.
func (e *Enodesv4) Get(index int) (enodev4 *Enodev4, _ error) {
	if index < 0 || index >= len(e.nodes) {
		return nil, errors.New("index out of bounds")
	}
	return &Enodev4{e.nodes[index]}, nil
}

// Set sets the enode at the given index in the slice.
func (e *Enodesv4) Set(index int, enode *Enodev4) error {
	if index < 0 || index >= len(e.nodes) {
		return errors.New("index out of bounds")
	}
	e.nodes[index] = enode.node
	return nil
}

// Append adds a new enode element to the end of the slice.
func (e *Enodesv4) Append(enode *Enodev4) {
	e.nodes = append(e.nodes, enode.node)
}
