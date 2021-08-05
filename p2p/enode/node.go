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

package enode

import (
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"math/bits"
	"math/rand"
	"net"

	"strings"

	"github.com/gauss-project/eswarm/p2p/enr"
)

type NodeTypeOption uint8

const (
	NodeTypeFull        NodeTypeOption = 0x24
	//NodeTypeLight                      = 0x11
	NodeTypeLightServer                = 0x21
	NodeTypeStorage                    = 0x22
	NodeTypeBoot                       = 0x09
)

func GetRetrievalOptions(nodeType NodeTypeOption) RetrievalOption {
	return RetrievalOption((nodeType >> 3) & 0x07)
}

func GetSyncingOptions(nodeType NodeTypeOption) SyncingOption {
	return SyncingOption((nodeType) & 0x07)
}

func IsLightNode(nodeType NodeTypeOption) bool {
	return  !(IsBootNode(nodeType) || IsFullNode(nodeType) || IsLightServer(nodeType) || IsStorageNode(nodeType))
}
func IsConnectableNode(nodeType NodeTypeOption) bool {
	return  IsBootNode(nodeType) || IsFullNode(nodeType) || IsLightServer(nodeType) || IsStorageNode(nodeType)
}

func IsBootNode(nodeType NodeTypeOption) bool {
	return GetRetrievalOptions(nodeType) == RetrievalDisabled && GetSyncingOptions(nodeType) == SyncingDisabled
}
func IsLightServer(nodeType NodeTypeOption) bool {
	return GetRetrievalOptions(nodeType) == RetrievalEnabled && GetSyncingOptions(nodeType) == SyncingDisabled
}
func IsStorageNode(nodeType NodeTypeOption) bool {
	return GetRetrievalOptions(nodeType) == RetrievalEnabled && GetSyncingOptions(nodeType) == SyncingRegisterOnly
}
func IsFullNode(nodeType NodeTypeOption) bool {
	return GetRetrievalOptions(nodeType) == RetrievalEnabled && GetSyncingOptions(nodeType) == SyncingAutoSubscribe
}

// Enumerate options for syncing and retrieval
type SyncingOption uint8
type RetrievalOption uint8

// Syncing options
const (
	// Syncing disabled
	SyncingDisabled SyncingOption = 1
	// Register the client and the server but not subscribe
	SyncingRegisterOnly SyncingOption = 2
	// Both client and server funcs are registered, subscribe sent automatically
	SyncingAutoSubscribe SyncingOption = 4
)

const (
	// Retrieval disabled. Used mostly for tests to isolate syncing features (i.e. syncing only)
	RetrievalDisabled RetrievalOption = 1
	// Only the client side of the retrieve request is registered.
	// (light nodes do not serve retrieve requests)
	// once the client is registered, subscription to retrieve request stream is always sent
	RetrievalClientOnly RetrievalOption = 2
	// Both client and server funcs are registered, subscribe sent automatically
	RetrievalEnabled RetrievalOption = 4
)

// Node represents a host on the network.
type Node struct {
	r  enr.Record
	id ID
}

// New wraps a node record. The record must be valid according to the given
// identity scheme.
func New(validSchemes enr.IdentityScheme, r *enr.Record) (*Node, error) {
	if err := r.VerifySignature(validSchemes); err != nil {
		return nil, err
	}
	node := &Node{r: *r}
	if n := copy(node.id[:], validSchemes.NodeAddr(&node.r)); n != len(ID{}) {
		return nil, fmt.Errorf("invalid node ID length %d, need %d", n, len(ID{}))
	}
	return node, nil
}

// ID returns the node identifier.
func (n *Node) ID() ID {
	return n.id
}

// Seq returns the sequence number of the underlying record.
func (n *Node) Seq() uint64 {
	return n.r.Seq()
}

// Incomplete returns true for nodes with no IP address.
func (n *Node) Incomplete() bool {
	return n.IP() == nil
}

// Load retrieves an entry from the underlying record.
func (n *Node) Load(k enr.Entry) error {
	return n.r.Load(k)
}

// IP returns the IP address of the node.
func (n *Node) LIP() net.IP {
	var ip net.IP
	n.Load((*enr.LocalIP)(&ip))
	return ip
}

func (n *Node) LUDP() uint16 {
	var port enr.LUDP
	n.Load(&port)
	return uint16(port)
}

// IP returns the IP address of the node.
func (n *Node) IP() net.IP {
	var ip net.IP
	n.Load((*enr.IP)(&ip))
	return ip
}

// IP returns the IP address of the node.
func (n *Node) NodeType() enr.NodeType {
	ln := uint8(0)
	n.Load((*enr.NodeType)(&ln))

	return enr.NodeType(ln)
}

// IP returns the IP address of the node.
func (n *Node) Set(val enr.Entry) {

	n.r.Set(val)

}

// IP returns the IP address of the node.
func (n *Node) SetNodeType(nodeType enr.NodeType) {

	n.r.Set((enr.NodeType)(nodeType))

}

// UDP returns the UDP port of the node.
func (n *Node) UDP() int {
	var port enr.UDP
	n.Load(&port)
	return int(port)
}

// UDP returns the TCP port of the node.
func (n *Node) TCP() int {
	var port enr.TCP
	n.Load(&port)
	return int(port)
}

// Pubkey returns the secp256k1 public key of the node, if present.
func (n *Node) Pubkey() *ecdsa.PublicKey {
	var key ecdsa.PublicKey
	if n.Load((*Secp256k1)(&key)) != nil {
		return nil
	}
	return &key
}

// Record returns the node's record. The return value is a copy and may
// be modified by the caller.
func (n *Node) Record() *enr.Record {
	cpy := n.r
	return &cpy
}

// checks whether n is a valid complete node.
func (n *Node) ValidateComplete() error {
	if n.Incomplete() {
		return errors.New("incomplete node")
	}
	if n.UDP() == 0 {
		return errors.New("missing UDP port")
	}
	ip := n.IP()
	if ip.IsMulticast() || ip.IsUnspecified() {
		return errors.New("invalid IP (multicast/unspecified)")
	}
	// Validate the node key (on curve, etc.).
	var key Secp256k1
	return n.Load(&key)
}

// The string representation of a Node is a URL.
// Please see ParseNode for a description of the format.
func (n *Node) String() string {
	return n.v4URL()
}

// MarshalText implements encoding.TextMarshaler.
func (n *Node) MarshalText() ([]byte, error) {
	return []byte(n.v4URL()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (n *Node) UnmarshalText(text []byte) error {
	dec, err := ParseV4(string(text))
	if err == nil {
		*n = *dec
	}
	return err
}

// ID is a unique identifier for each node.
type ID [32]byte

// Bytes returns a byte slice representation of the ID
func (n ID) Bytes() []byte {
	return n[:]
}

// ID prints as a long hexadecimal number.
func (n ID) String() string {
	return fmt.Sprintf("%x", n[:])
}

// The Go syntax representation of a ID is a call to HexID.
func (n ID) GoString() string {
	return fmt.Sprintf("enode.HexID(\"%x\")", n[:])
}

// TerminalString returns a shortened hex string for terminal logging.
func (n ID) TerminalString() string {
	return hex.EncodeToString(n[:8])
}

// MarshalText implements the encoding.TextMarshaler interface.
func (n ID) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(n[:])), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (n *ID) UnmarshalText(text []byte) error {
	id, err := parseID(string(text))
	if err != nil {
		return err
	}
	*n = id
	return nil
}

// HexID converts a hex string to an ID.
// The string may be prefixed with 0x.
// It panics if the string is not a valid ID.
func HexID(in string) ID {
	id, err := parseID(in)
	if err != nil {
		panic(err)
	}
	return id
}

func parseID(in string) (ID, error) {
	var id ID
	b, err := hex.DecodeString(strings.TrimPrefix(in, "0x"))
	if err != nil {
		return id, err
	} else if len(b) != len(id) {
		return id, fmt.Errorf("wrong length, want %d hex chars", len(id)*2)
	}
	copy(id[:], b)
	return id, nil
}

// DistCmp compares the distances a->target and b->target.
// Returns -1 if a is closer to target, 1 if b is closer to target
// and 0 if they are equal.
func DistCmp(target, a, b ID) int {
	for i := range target {
		da := a[i] ^ target[i]
		db := b[i] ^ target[i]
		if da > db {
			return 1
		} else if da < db {
			return -1
		}
	}
	return 0
}

// LogDist returns the logarithmic distance between a and b, log2(a ^ b).
func LogDist(a, b ID) int {
	lz := 0
	for i := range a {
		x := a[i] ^ b[i]
		if x == 0 {
			lz += 8
		} else {
			lz += bits.LeadingZeros8(x)
			break
		}
	}
	return len(a)*8 - lz
}

// RandomID returns a random ID b such that logdist(a, b) == n.
func RandomID(a ID, n int) (b ID) {
	if n == 0 {
		return a
	}
	// flip bit at position n, fill the rest with random bits
	b = a
	pos := len(a) - n/8 - 1
	bit := byte(0x01) << (byte(n%8) - 1)
	if bit == 0 {
		pos++
		bit = 0x80
	}
	b[pos] = a[pos]&^bit | ^a[pos]&bit // TODO: randomize end bits
	for i := pos + 1; i < len(a); i++ {
		b[i] = byte(rand.Intn(255))
	}
	return b
}
