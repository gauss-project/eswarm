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

// Contains all the wrappers from the node package to support client side node
// management on mobile platforms.

package csdc

//import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gauss-project/eswarm/accounts"
	"github.com/gauss-project/eswarm/accounts/keystore"
	"github.com/gauss-project/eswarm/common"
	"github.com/gauss-project/eswarm/log"
	"github.com/gauss-project/eswarm/node"
	"github.com/gauss-project/eswarm/p2p"
	"github.com/gauss-project/eswarm/p2p/nat"
	"github.com/gauss-project/eswarm/swarm"
	bzzapi "github.com/gauss-project/eswarm/swarm/api"
	"github.com/gauss-project/eswarm/swarm/util"
	"github.com/gauss-project/eswarm/swarm/version"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	rm "runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func init() {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Error("Error Getting Rlimit ", "get", err)
	}
	rLimit.Cur = rLimit.Max
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Error("Error Setting Rlimit ", "set", err)
	}
	log.Info("Rlimit Final", "limits", rLimit)
}

// NodeConfig represents the collection of configuration values to fine tune the Geth
// node embedded into a mobile process. The available values are a subset of the
// entire API provided by go-ethereum to reduce the maintenance surface and dev
// complexity.
type NodeConfig struct {
	// Bootstrap nodes used to establish connectivity with the rest of the network.
	//BootstrapNodes *Enodesv4

	// MaxPeers is the maximum number of peers that can be connected. If this is
	// set to zero, then only the configured static and trusted peers can connect.
	MaxPeers int

	// EthereumEnabled specifies whether the node should run the Ethereum protocol.
	EthereumEnabled bool

	// EthereumNetworkID is the network identifier used by the Ethereum protocol to
	// decide if remote peers should be accepted or not.
	EthereumNetworkID int64 // uint64 in truth, but Java can't handle that...

	// SwarmEnabled specifies whether the node should run the Swarm protocol.
	SwarmEnabled bool

	// SwarmAccount specifies account ID used for starting Swarm node.
	SwarmAccount string

	// SwarmAccountPassword specifies password for account retrieval from the keystore.
	SwarmAccountPassword string

	ServerAddrs []string
}

// defaultNodeConfig contains the default node configuration values to use if all
// or some fields are missing from the user's specified list.
var defaultNodeConfig = &NodeConfig{
	MaxPeers:          25,
	EthereumEnabled:   false,
	EthereumNetworkID: 1278,
}

// NewNodeConfig creates a new node option set, initialized to the default values.
func NewNodeConfig() *NodeConfig {
	config := *defaultNodeConfig
	return &config
}

// Node represents a Geth Ethereum node instance.
type Node struct {
	node     *node.Node
	httpPort string
}

type ActivatePost struct {
	Appid      string
	Credential string
	Account    string
	ClientId   string
}

type RespData struct {
	ExpireTime int64  `json:"expireTime"`
	NodeUrl    string `json:"nodeUrl"`
	BootNode   string `json:"bootNode"`
	Error      string `json:"error"`
}

func PostToServer(urlStr string, timeout time.Duration, data *ActivatePost) (*RespData, error) {

	client := &http.Client{
		Timeout: timeout * time.Second,
	}

	postdata := make(url.Values)
	postdata.Set("appId", data.Appid)
	postdata.Set("account", data.Account)
	postdata.Set("credential", data.Credential)
	postdata.Set("clientId", data.ClientId)
	postdata.Set("version", version.Version)
	postdata.Set("vMeta", version.VersionMeta)
	postdata.Set("os", rm.GOOS)
	log.Info("activate started ", "params", data)
	resp, err := client.PostForm(urlStr, postdata)
	respData := RespData{}
	if err != nil {
		return &respData, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return &respData, err
	}

	err = json.Unmarshal(body, &respData)
	if err != nil {
		return &respData, err
	}
	if resp.StatusCode != 200 {
		return &respData, fmt.Errorf("resp.Body: %v", respData.Error)
	}

	return &respData, nil
}
func Start(path string, password string, bootnodeAddrs string, bootnode string) (stack *Node, _ error) {
	return StartL(path, password, bootnodeAddrs, bootnode, 1)
}
func StartL(path string, password string, bootnodeAddrs string, bootnode string, logLevel int) (stack *Node, _ error) {
	//path keystore上一级目录
	glogger := log.NewGlogHandler(log.StreamHandler(os.Stderr, log.TerminalFormat(false)))
	glogger.Verbosity(log.Lvl(logLevel))
	log.Root().SetHandler(glogger)

	if path == "" {
		return nil, errors.New("Must input path ...")
	}

	if password == "" {
		password = "123"
	}

	var account string
	fileinfo, err := ioutil.ReadDir(path + "/keystore")
	if err != nil && len(fileinfo) == 0 {
		return nil, errors.New("Please input the correct directory or keystore dir is empty. Please activate first...")
	}
	for _, file := range fileinfo {
		if !file.IsDir() { //UTC--2019-03-26T10-26-19.431075000Z--3de7c66e10c0f476c9b0d221e9cf1affd50eeac2
			account = file.Name()
			account = account[len(account)-40:]
			break
		}
	}

	bootnodes := []string{}

	config := NewNodeConfig()
	config.SwarmAccount = account
	config.SwarmAccountPassword = password

	//var rLimit syscall.Rlimit
	//rErr := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	//if rErr != nil {
	//	return nil, errors.New("rLimit error")
	//	log.Error("start Error Getting Rlimit ", "get", err)
	//} else {
	//	if rLimit.Cur < 2048 {
	//		return nil, errors.New("rLimit error")
	//	}
	//}

	if bootnodeAddrs != "" {
		nodes, reportUrl, err := util.GetBootnodesInfo(bootnodeAddrs)
		if err == nil {
			if len(nodes) != 0 {
				bootnodes = append(bootnodes, nodes...)
			}
		}
		config.ServerAddrs = reportUrl
	}

	stack, err = NewSwarmNode(path, config)
	if err != nil {
		return nil, errors.New("NewSwarmNode func err...")
	}

	if swarmErr := stack.Start(); swarmErr != nil {
		return nil, swarmErr
	}

	stack.GetNodeInfo()

	if bootnode != "" {
		bootnodes = append(bootnodes, bootnode)

	}

	for _, boot := range bootnodes {
		stack.AddBootnode(boot)
	}

	return stack, nil
}

func CreateKeyStore(path, passwd string, ScryptN, ScryptP int) (string, error) {
	keystore := NewKeyStore(path, ScryptN, ScryptP)
	account, err := keystore.NewAccount(passwd)
	if err != nil {
		return "", errors.New("keystore.NewAccount func err...")
	}
	return account.GetAddress().GetHex(), nil
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func Activate(path string, appId string, credential string, addr string, newAccount bool, password string, arg int) (*RespData, error) {
	return ActivateR(path, appId, "", credential, addr, newAccount, password, arg)
}

func ActivateR(path string, appId string, clientId string, credential string, addr string, newAccount bool, password string, arg int) (*RespData, error) {

	if path == "" {
		return &RespData{}, errors.New("Must input path ...")
	}

	//清除缓存
	err := Clean(path, arg)
	if err != nil {
		return &RespData{}, errors.New("Clean cache fail ...")
	}

	if addr == "" {
		addr = "https://service.cdscfund.org/apis/v1/activate"
	}

	if password == "" {
		password = "123"
	}

	keystorepath := path + "/keystore/"
	exist, err := PathExists(keystorepath)
	if err != nil {
		return &RespData{}, err
	}
	if !exist {
		err = os.Mkdir(path+"/keystore", os.ModePerm)
		if err != nil {
			return &RespData{}, err
		}
	}

	bzzAccount := ""
	fileinfo, err := ioutil.ReadDir(keystorepath)
	if err != nil {
		return &RespData{}, err
	}
	for _, file := range fileinfo {
		if !file.IsDir() {
			//UTC--2019-03-26T10-26-19.431075000Z--3de7c66e10c0f476c9b0d221e9cf1affd50eeac2
			if newAccount == false {
				bzzAccount = file.Name()
				bzzAccount = bzzAccount[len(bzzAccount)-40:]
				bzzAccount = "0x" + bzzAccount
			} else { //重新生成一个keystore
				err := os.Remove(keystorepath + file.Name())
				if err != nil {
					return &RespData{}, err
				}
			}

			break
		}
	}

	if bzzAccount == "" {
		account, err := CreateKeyStore(keystorepath, password, LightScryptN, LightScryptP)
		if err != nil {
			return &RespData{}, err
		}
		bzzAccount = account

		log.Info("account created:", "bzzAccount", account)
	}

	activatePost := &ActivatePost{appId, credential, bzzAccount, clientId}
	ti, err := PostToServer(addr, 5, activatePost)
	if err != nil {
		return &RespData{}, err
	}
	return ti, err
}

func Clean(path string, arg int) error {
	//path keystore上一级目录
	//arg 0:删除path下所有，1：删除bzz缓存，2：删除SwarmMobile目录，3：删除SwarmMobile下nodes目录
	if path == "" {
		return errors.New("Must input path or platform（android or ios）...")
	}
	exist, err := PathExists(path)
	if err != nil {
		return nil
	}
	if !exist {
		return nil
	}

	if arg == 2 {
		err := os.RemoveAll(path + "SwarmMobile")
		if err != nil {
			return fmt.Errorf("Clear fail: %v", err)
		}
		return nil
	} else if arg == 3 {
		err := os.RemoveAll(path + "SwarmMobile/nodes")
		if err != nil {
			return fmt.Errorf("Clear fail: %v", err)
		}
		return nil
	}

	fileinfo, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, file := range fileinfo {
		if arg == 0 {
			err := os.RemoveAll(path + file.Name())
			if err != nil {
				return fmt.Errorf("Clear fail: %v", err)
			}
		} else if arg == 1 {
			if len(file.Name()) == 44 && file.Name()[:4] == "bzz-" {
				err := os.RemoveAll(path + file.Name())
				if err != nil {
					return fmt.Errorf("Clear fail: %v", err)
				}
			}
		}
	}

	return nil

}

// getSwarmKey is a helper for finding and decrypting given account's private key
// to be used with Swarm.
func getSwarmKey(stack *node.Node, account string, password string) (*keystore.Key, error) {
	if account == "" {
		return nil, nil // it's not an error, just skip
	}

	address := common.HexToAddress(account)
	acc := accounts.Account{
		Address: address,
	}

	am := stack.AccountManager()
	ks := am.Backends(keystore.KeyStoreType)[0].(*keystore.KeyStore)
	a, err := ks.Find(acc)
	if err != nil {
		return nil, fmt.Errorf("find account: %v", err)
	}
	keyjson, err := ioutil.ReadFile(a.URL.Path)
	if err != nil {
		return nil, fmt.Errorf("load key: %v", err)
	}

	return keystore.DecryptKey(keyjson, password)
}

func NewSwarmNode(datadir string, config *NodeConfig) (stack *Node, _ error) {
	if config == nil {
		config = NewNodeConfig()
	}
	if config.MaxPeers == 0 {
		config.MaxPeers = defaultNodeConfig.MaxPeers
	}

	// Create the empty networking stack
	nodeConf := &node.Config{
		Name: clientIdentifier,
		//Version:     params.VersionWithMeta,
		DataDir: datadir,
		//IPCPath:     "bzzd.ipc",
		KeyStoreDir: filepath.Join(datadir, "keystore"), // Mobile should never use internal keystores!
		P2P: p2p.Config{
			NoDiscovery: false,
			NoDial:      true,
			ListenAddr:  ":0",
			NAT:         nat.Any(),
			MaxPeers:    config.MaxPeers,
			NodeType:    17,
		},
		NoUSB:    true,
		NodeType: 17,
	}

	rawStack, err := node.New(nodeConf)
	if err != nil {
		return nil, err
	}

	anode := &Node{rawStack, "0"}
	if err := rawStack.Register(func(*node.ServiceContext) (node.Service, error) {
		bzzconfig := bzzapi.NewConfig()

		if anode.httpPort == "0" {
			// start swarm http proxy server
			l, _ := net.Listen("tcp", ":0") // listen on localhost
			port := l.Addr().(*net.TCPAddr).Port
			bzzconfig.Port = strconv.Itoa(port)
			l.Close()
		} else {
			bzzconfig.Port = anode.httpPort
		}

		anode.SetHttpPort(bzzconfig.Port)
		bzzconfig.Path = datadir
		bzzconfig.NodeType = 17
		bzzconfig.LocalStoreParams.DbCapacity = 0      //120M
		bzzconfig.LocalStoreParams.CacheCapacity = 100 //26M
		if len(config.ServerAddrs) != 0 {
			bzzconfig.ServerAddrs = config.ServerAddrs
		}
		key, err := getSwarmKey(rawStack, config.SwarmAccount, config.SwarmAccountPassword)
		if err != nil {
			return nil, fmt.Errorf("no key")
		}
		bzzconfig.Init(key.PrivateKey)

		bzzconfig.BzzAccount = config.SwarmAccount
		return swarm.NewSwarm(bzzconfig, nil)
	}); err != nil {
		return nil, fmt.Errorf("swarm init: %v", err)
	}

	return anode, nil
}

// Close terminates a running node along with all it's services, tearing internal
// state doen too. It's not possible to restart a closed node.
func (n *Node) Close() error {
	return n.node.Close()
}

// Start creates a live P2P node and starts running it.
func (n *Node) Start() error {
	return n.node.Start()
}

// Stop terminates a running node along with all it's services. If the node was
// not started, an error is returned.
func (n *Node) Stop() error {
	return n.node.Stop()
}

func (n *Node) AddPeer(peer *Enodev4) {
	n.node.Server().AddPeer(peer.node)
}

func DefaultDataDir() string {
	return node.DefaultDataDir()
}

func (n *Node) AddBootnode(enodeStr string) error {

	enode, err := NewEnodev4(enodeStr)
	if err != nil {
		return err
	}
	n.AddPeer(enode)
	return nil
}

// GetNodeInfo gathers and returns a collection of metadata known about the host.
func (n *Node) GetNodeInfo() *NodeInfo {
	return &NodeInfo{n.node.Server().NodeInfo()}
}

// GetPeersInfo returns an array of metadata objects describing connected peers.
func (n *Node) GetPeersInfo() *PeerInfos {
	return &PeerInfos{n.node.Server().PeersInfo()}
}

func (n *Node) SetHttpPort(port string) {
	n.httpPort = port
}

// getBzzPort
func (n *Node) GetHttpPort() string {
	return n.httpPort
}

// getM3U8baseUrl
func (n *Node) GetM3U8BaseUrl() string {

	return fmt.Sprintf("http://localhost:%v/m3u8:/", n.httpPort)
}

// getM3U8 url
func (n *Node) GetM3U8Url(cdnUrl string, hash string) string {

	if hash == "" {
		return cdnUrl
	}

	index := strings.LastIndex(cdnUrl, "/")
	baseUrl := n.GetM3U8BaseUrl()

	url := fmt.Sprintf("%v/{%v}%v", cdnUrl[:index], hash, cdnUrl[index:])
	url = strings.Replace(url, "://", "/", -1)

	return fmt.Sprintf("%v%v", baseUrl, url)
}

// getFile baseUrl
func (n *Node) GetFileBaseUrl() string {
	return fmt.Sprintf("http://localhost:%v/file:/", n.httpPort)
}

// getM3U8 url
func (n *Node) GetFileUrl(cdnUrl string, hash string) string {

	if hash == "" {
		return cdnUrl
	}

	index := strings.LastIndex(cdnUrl, "/")
	baseUrl := n.GetFileBaseUrl()

	url := fmt.Sprintf("%v/{%v}%v", cdnUrl[:index], hash, cdnUrl[index:])
	url = strings.Replace(url, "://", "/", -1)

	return fmt.Sprintf("%v%v", baseUrl, url)
}
