package state

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gauss-project/eswarm/common"
	"github.com/gauss-project/eswarm/crypto"
	"github.com/gauss-project/eswarm/swarm/util"
	"golang.org/x/crypto/sha3"

	"github.com/hashicorp/golang-lru"
	"github.com/gauss-project/eswarm/p2p/enode"
	"github.com/gauss-project/eswarm/rlp"
	"github.com/gauss-project/eswarm/swarm/log"
	"github.com/syndtr/goleveldb/leveldb"
	dberrors "github.com/syndtr/goleveldb/leveldb/errors"
)

var (
	ErrInvalidNode       = errors.New("InvalidNodeId")
	ErrUnexpectedReceipt = errors.New("UnexpectedReceipt")
	ErrInvalidSignature  = errors.New("InvalidSignature")
	ErrInvalidSTime      = errors.New("InvalidSignTime")
)

const (
	MAX_C_REC_LIMIT = 4096 //当超过这个数目时，最长时间不用的C_记录，就是找了最久没有连接的节点

	ReportRoute  = "/receipts"
	AccountRoute = "/account"
)

var (
	CPREF              = []byte("IN_CHUNK")
	HPREF              = []byte("IN_RECEIPT")
	RPREF              = []byte("UNREPORTED")
	SDATE			   = []byte("SDATE")
	SFORMAT			   = "2010-01-01 03:11:15"
	BALNACE_PREFIX     = "BL"
	MAX_STIME_DURATION = 60 * time.Minute       //生成收据时，一个STIME允许的最长时间
	MAX_STIME_JITTER   = 2 * MAX_STIME_DURATION //接收收据时，允许最长的时间差，超过这个时间的不再接收
	MAX_ITEM_PER_REPORT = 2000

)

type ChunkDeliverItem struct {
	FromTime  time.Time //从某个时间点开始
	FromCount uint32    //从某个数值开始
	Delivered uint32    //已经发送的数据包
	unpayed   uint32    //没有收到收据的，发送数据包数量-签收数据包数量的差值
}

type ChunkDeliverInfo map[enode.ID]*ChunkDeliverItem

//收据的数据
type ReceiptData struct {
	Stime     time.Time
	Amount    uint32
	Signature []byte
}
type rlpRD struct {
	Stime     uint32
	Amount    uint32
	Signature []byte
}
type ReceiptWatcher interface {
	//收到了receipts
	OnNewReceipts(address common.Address,id enode.ID,length int)
	ID() string
}
func (r ReceiptData) EncodeRLP(w io.Writer) error {

	rs := &rlpRD{uint32(r.Stime.Unix()), r.Amount, r.Signature}

	return rlp.Encode(w, rs)
}
func (rs *ReceiptData) DecodeRLP(s *rlp.Stream) error {
	result := new(rlpRD)
	err := s.Decode(result)
	if err == nil {
		rs.Signature = result.Signature
		rs.Stime = time.Unix(int64(result.Stime), 0)
		rs.Amount = result.Amount
	}
	return err
}

//收据的数据
type ReceiptBody struct {
	Account [20]byte //数据提供者
	Stime   time.Time
	Amount  uint32
}
type rlpRB struct {
	Account [20]byte
	Stime   uint32
	Amount  uint32
}

func (r ReceiptBody) EncodeRLP(w io.Writer) error {

	rs := &rlpRB{r.Account, uint32(r.Stime.Unix()), r.Amount}

	return rlp.Encode(w, rs)
}
func (rs *ReceiptBody) DecodeRLP(s *rlp.Stream) error {
	result := new(rlpRB)
	err := s.Decode(result)
	if err == nil {
		rs.Account = result.Account
		rs.Stime = time.Unix(int64(result.Stime), 0)
		rs.Amount = result.Amount
	}
	return err
}

//某个单个的收据
type Receipt struct {
	ReceiptBody
	Sign []byte
}
type ReceiptInStore struct {
	Stime  time.Time
	Amount uint32
}

type Hash [32]byte

func rlpHash(x interface{}) (h Hash) {
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, x)
	hw.Sum(h[:0])
	return h
}

/**
签名
*/
func (r *Receipt) Signature(prvKey *ecdsa.PrivateKey) error {
	h := rlpHash(r.ReceiptBody)
	sig, err := crypto.Sign(h[:], prvKey)
	if err != nil {
		return err
	}
	r.Sign = sig
	return nil
}

/**
验证签名是否正确，并且返回签名者的公钥
*/
func (r *Receipt) Verify() (*ecdsa.PublicKey, bool) {
	//SigToPub
	//bodyData, _ := rlp.EncodeToBytes(r.ReceiptBody)
	//fmt.Println("array is:", bodyData)
	h := rlpHash(r.ReceiptBody)
	//fmt.Println("hash:", h)
	pubKey, err := crypto.SigToPub(h[:], r.Sign)
	if err == nil {
		pubKeyBytes := crypto.CompressPubkey(pubKey)
		if crypto.VerifySignature(pubKeyBytes, h[:], r.Sign[:64]) {
			return pubKey, true
		}
	}
	return nil, false
}

type ReceiptItem struct {
	Amount uint32
	Sign   []byte
}
type ReceiptItems map[time.Time]ReceiptItem

func (rs *ReceiptItems) Amount() (amount uint32) {
	amount = 0
	for _, val := range *rs {
		amount += val.Amount
	}
	return
}
func (rs *ReceiptItems) EncodeRLP(w io.Writer) error {

	rcs := make([]ReceiptData, 0)
	for id, item := range *rs {
		rcItem := ReceiptData{
			id,
			item.Amount,
			item.Sign,
		}
		rcs = append(rcs, rcItem)
	}
	return rlp.Encode(w, rcs)
}
func (rs *ReceiptItems) DecodeRLP(s *rlp.Stream) error {
	result := new([]*ReceiptData)
	err := s.Decode(result)
	if err == nil {
		for _, item := range *result {
			(*rs)[item.Stime] = ReceiptItem{item.Amount, item.Signature}
		}
	}
	return err
}

//某个来源节点的收据集
type ReceiptsOfNode struct {
	NodeId   [20]byte
	Receipts []*ReceiptData
}

type Receipts map[[20]byte]ReceiptItems

/**
 *   测试用，计算总的收据的数量
 */
func (rs Receipts) Amount() (amount uint32) {
	amount = 0
	for _, items := range rs {
		amount += items.Amount()
	}
	return
}
func (rs Receipts) EncodeRLP(w io.Writer) error {

	rcs := make([]*ReceiptsOfNode, 0)
	for id, item := range rs {
		recsOfNode := make([]*ReceiptData, 0)
		for stime, data := range item {
			recsOfNode = append(recsOfNode, &ReceiptData{stime, data.Amount, data.Sign})
		}

		rcItem := &ReceiptsOfNode{
			id,
			recsOfNode,
		}
		rcs = append(rcs, rcItem)
	}
	return rlp.Encode(w, rcs)
}
func (rs *Receipts) DecodeRLP(s *rlp.Stream) error {
	result := make([]*ReceiptsOfNode, 0)
	err := s.Decode(&result)
	if err == nil {
		for _, item := range result {
			items := make(ReceiptItems)
			for _, eachReceiptItem := range item.Receipts {
				items[eachReceiptItem.Stime] = ReceiptItem{eachReceiptItem.Amount, eachReceiptItem.Signature}
			}
			(*rs)[item.NodeId] = items
		}
	}
	return err
}

func (rs *Receipts) CurrentReceipt(account [20]byte) *ReceiptData {

	result, ok := (*rs)[account]
	if !ok {
		return nil
	}
	lastestTime := time.Now().AddDate(-10, 0, 0)
	for sTime, _ := range result {
		if sTime.After(lastestTime) {
			lastestTime = sTime
		}
	}
	return &ReceiptData{lastestTime, result[lastestTime].Amount, result[lastestTime].Sign}
}

type ReceiptStore struct {
	account     [20]byte
	hex         string
	db          *leveldb.DB
	allReceipts Receipts
	prvKey      *ecdsa.PrivateKey
	//deliverInfo ChunkDeliverInfo
	unpaidAmount  map[[20]byte]uint32
	nodeCommCache *lru.Cache
	cmu           sync.RWMutex
	hmu           sync.RWMutex
	servers        []string
	checkBalance  bool
	receiptsLogs  []Receipts
	balances      *lru.Cache
	watchers 	 map[string]ReceiptWatcher
	lightNode     bool
	quit         chan struct{}
	filePath     string
	saveHTimer    *time.Ticker
}

func NewReceiptsStore(filePath string, prvKey *ecdsa.PrivateKey, serverAddrs []string, duration time.Duration, checkBalance bool,lightNode bool) (*ReceiptStore, error) {

	MAX_STIME_DURATION = duration             //生成收据时，一个STIME允许的最长时间
	MAX_STIME_JITTER = 2 * MAX_STIME_DURATION //接收收据时，允许最长的时间差，超过这个时间的不再接收
	return newReceiptsStore(filePath, prvKey, serverAddrs, checkBalance,lightNode), nil
}
func newReceiptsStore(filePath string , prvKey *ecdsa.PrivateKey, serverAddrs []string, checkBalance bool,lightNode bool) *ReceiptStore {
	balances, _ := lru.New(100)
	store := ReceiptStore{
		account:      crypto.PubkeyToAddress(prvKey.PublicKey),
		hex:          crypto.PubkeyToAddress(prvKey.PublicKey).Hex(),
		prvKey:       prvKey,
		unpaidAmount: make(map[[20]byte]uint32),
		allReceipts:  make(Receipts),
		servers:       serverAddrs,
		receiptsLogs: make([]Receipts, 0),
		checkBalance: checkBalance,
		balances:     balances,
		watchers:     make(map[string]ReceiptWatcher),
		lightNode:	  lightNode,
		filePath:     filePath,
	}
	store.nodeCommCache, _ = lru.New(MAX_C_REC_LIMIT)


//	store.Start()


	return &store
}
func (rs *ReceiptStore) Start(){

	if rs.db != nil {
		rs.db.Close()
	}

	db, err := leveldb.OpenFile(rs.filePath, nil)
	if _, iscorrupted := err.(*dberrors.ErrCorrupted); iscorrupted {
		db, err = leveldb.RecoverFile(rs.filePath, nil)
	}
	rs.db = db
	rs.quit = make(chan struct{})

	rs.Init()
	if !rs.lightNode {
		go rs.submitRoutine()
	}

	rs.saveHTimer = time.NewTicker(5*time.Minute)
	go func() {
		for  range rs.saveHTimer.C {
			rs.saveHRecord()
		}
	}()
}
func (rs *ReceiptStore) Stop(){
	rs.saveHTimer.Stop();
	close(rs.quit)

	if rs.db != nil {
		rs.db.Close()
	}

}
func (rs *ReceiptStore) saveHRoutine(){

}
func (rs *ReceiptStore) SetNewWather(watcher ReceiptWatcher)  {
	rs.watchers[watcher.ID()] = watcher
}
func (rs *ReceiptStore) Account() [20]byte {
	return rs.account
}
func (rs *ReceiptStore) Init() {
	rs.loadCRecord()
	rs.loadHRecord()

}

func (rs *ReceiptStore) loadCRecord() {
	data, err := rs.db.Get(CPREF, nil)
	result := make([]*ReceiptBody, 0)
	if err == nil {
		err = rlp.DecodeBytes(data, &result)
		if err == nil {
			rs.nodeCommCache.Purge()
			for _, item := range result {
				rs.nodeCommCache.ContainsOrAdd(item.Account, &ReceiptInStore{item.Stime, item.Amount})
			}
		}
	}
}
func (rs *ReceiptStore) saveCRecord() error {
	results := make([]*ReceiptBody, 0)
	allIds := rs.nodeCommCache.Keys()
	for _, account := range allIds {
		item, exist := rs.nodeCommCache.Get(account)
		if exist {
			receipt := item.(*ReceiptInStore)
			results = append(results, &ReceiptBody{account.([20]byte), receipt.Stime, receipt.Amount})
		}

	}
	data, err := rlp.EncodeToBytes(results)
	err = rs.db.Put(CPREF, data, nil)
	return err
}
func (rs *ReceiptStore) loadHRecord() {
	rs.allReceipts = rs.loadReceipts(HPREF)
}
func (rs *ReceiptStore) saveHRecord() error {
	//持久化数据

	return rs.saveReceipts(HPREF, rs.allReceipts)
}

func (rs *ReceiptStore) loadReceipts(key []byte) Receipts {

	data, err := rs.db.Get(key, nil)
	result := make(Receipts)
	if err == nil {
		err = rlp.DecodeBytes(data, &result)
		if err == nil {
			return result
		}
	}
	return result
}
func (rs *ReceiptStore) saveReceipts(key []byte, receipts Receipts) error {
	//持久化数据

	data, err := rlp.EncodeToBytes(receipts)
	err = rs.db.Put(key, data, nil)
	return err
}

//新收到了一个数据,在C记录中记录，并且返回一个签过名的收据
//如果nodeId不合法，返回的收据为空，error为ErrInvalidNode
func (rs *ReceiptStore) OnNodeChunkReceived(account [20]byte, dataLength int64) (*Receipt, error) {
	rs.cmu.Lock()
	defer rs.cmu.Unlock()

	if len(account) != 20 {
		return nil, ErrInvalidNode
	}
	chunkAmount := uint32((dataLength + 4095) >> 12)
	//update chunkOfNode
	item, exist := rs.nodeCommCache.Get(account)
	newTime := time.Now()
	if !exist {
		sDate,err := rs.db.Get(SDATE,nil)
		if err != nil {
			sTime,err := time.Parse(SFORMAT,string(sDate))
			if err != nil && MAX_STIME_DURATION < time.Since(sTime) {
				newTime = sTime
			}else{
				rs.db.Put(SDATE,[]byte(newTime.Format(SFORMAT)),nil)
			}
		}else{
			rs.db.Put(SDATE,[]byte(newTime.Format(SFORMAT)),nil)
		}
		item = &ReceiptInStore{newTime, chunkAmount}
	} else {
		if MAX_STIME_DURATION < time.Since(item.(*ReceiptInStore).Stime) {
			item = &ReceiptInStore{newTime, chunkAmount}
			rs.db.Put(SDATE,[]byte(newTime.Format(SFORMAT)),nil)
		} else {
			item = &ReceiptInStore{item.(*ReceiptInStore).Stime, item.(*ReceiptInStore).Amount + chunkAmount}
		}

	}
	rs.nodeCommCache.Add(account, item)
	//持久化
	rs.saveCRecord()
	//创建收据
	aReceipt := &Receipt{ReceiptBody{account, item.(*ReceiptInStore).Stime, item.(*ReceiptInStore).Amount}, []byte{}}

	aReceipt.Signature(rs.prvKey)
	return aReceipt, nil

}

func (rs *ReceiptStore)CalcReceipts()	error{

	return nil
}
//服务端新到了一个收据
func (rs *ReceiptStore) OnNewReceipt(id enode.ID,receipt *Receipt) error {
	rs.hmu.Lock()
	defer rs.hmu.Unlock()


	//不是自己的nodeId不收
	if receipt.Account != rs.account {
		return ErrInvalidNode
	}
	//超过MAX_STIME_JITTER(默认2个小时)的不收
	jitter := time.Since(receipt.Stime)
	if jitter > 24*time.Hour || jitter < -24*time.Hour {
		log.Error("signed time is :", "time", receipt.Stime)
		return ErrInvalidSTime
	}
	//验证签名是否正确
	pubKey, isOk := receipt.Verify()
	if !isOk {
		return ErrInvalidSignature
	}
	//根据这个pubKey生成nodeId
	nodeId := crypto.PubkeyToAddress(*pubKey)
	_, ok := rs.allReceipts[nodeId]
	//这个节点的第一次记录
	if !ok {
		rs.allReceipts[nodeId] = make(ReceiptItems)
	}
	_, ok = rs.allReceipts[nodeId][receipt.Stime]
	if !ok {
		//这个节点的这个STIME的第一次记录
		rs.allReceipts[nodeId][receipt.Stime] = ReceiptItem{receipt.Amount, receipt.Sign}
		rs.decreaseOnNewReceipt(nodeId, receipt.Amount)
	} else {
		//这个节点的这个STIME记录存在，只有更大的Amount才会覆盖小的
		if receipt.Amount > rs.allReceipts[nodeId][receipt.Stime].Amount {
			//更新未支付的数量
			rs.decreaseOnNewReceipt(nodeId, receipt.Amount-rs.allReceipts[nodeId][receipt.Stime].Amount)
			//覆盖原有的记录
			rs.allReceipts[nodeId][receipt.Stime] = ReceiptItem{receipt.Amount, receipt.Sign}
		}
	}
	for _,watcher := range rs.watchers {
		watcher.OnNewReceipts(nodeId,id,1)
	}
	//持久化
	return nil//rs.saveHRecord()
}
func (rs *ReceiptStore) GetReceiptsLogs() ([]Receipts) {

	toReport,_ := rs.GetReceiptsToReport()
	rs.hmu.Lock()
	defer rs.hmu.Unlock()
	result := make([]Receipts, 0)

	result = append(result, rs.receiptsLogs...)
	result = append(result, toReport)
	result = append(result, rs.allReceipts)
	return result
}
func (rs *ReceiptStore) GetReceiptsToReport() (Receipts,int) {
	rs.hmu.Lock()
	defer rs.hmu.Unlock()
	toReport,totalCnt := rs.extractReportReceipts()
	//从数据库中检查是否有上一次未提交成功的
	fromDB := rs.loadReceipts(RPREF)
	//合并
	for nodeId, items := range fromDB {
		newItems, ok := toReport[nodeId]

		if !ok {
			toReport[nodeId] = items
		} else {
			for stime, data := range items {
				if int(data.Amount) > MAX_ITEM_PER_REPORT {
					newItems[stime] = data
					fmt.Println(stime.Format("2006-01-02 15:04:05"))
				}

			}
		}
	}
	//持久化
	rs.saveReceipts(RPREF, toReport)
	return toReport,totalCnt
}

/**
	从库中找出所有的可以提交(stime超过两个小时的，）的收据
	遍历allReceipts，把超过2小时的和小于两小时的放到两个map里
    超过两小时的返回，小于两个小时的那个替换当前的allReceipts
*/
func (rs *ReceiptStore) extractReportReceipts() (Receipts,int) {

	result := make(Receipts)
	newReceipts := make(Receipts)
	total := 0
	for nodeId, receipts := range rs.allReceipts {
		for stime, receiptItem := range receipts {
			if time.Since(stime) > MAX_STIME_JITTER  { //超过两小时的
				receiptItems, ok := result[nodeId]
				if !ok {
					receiptItems = make(ReceiptItems)
					result[nodeId] = receiptItems
				}
				if receiptItem.Amount > 25 {
					receiptItems[stime] = receiptItem
					total++
				}


			} else { //小于两个小时的
				receiptItems, ok := newReceipts[nodeId]
				if !ok {
					receiptItems = make(ReceiptItems)
					newReceipts[nodeId] = receiptItems
				}
				receiptItems[stime] = receiptItem
			}
		}
	}
	if len(result) > 0 {
		rs.allReceipts = newReceipts
		rs.saveHRecord()
	}
	return result,total
}

type ReceiptsOfReport struct {
	Version  byte
	Account  [20]byte
	Receipts []rlpRD
}

func (rs *ReceiptStore) createReportData(receipts Receipts) (result [][]byte, err error) {


	totalSeg := (len(receipts)+4095) >> 12
	receiptsArrays := make([][]rlpRD, totalSeg)
	index := 0
	for _, item := range receipts {
		seg := (index >>12)
		//offset := (index & 0xFFF)
		for stime, val := range item {
			receiptsArrays[seg] = append(receiptsArrays[seg], rlpRD{uint32(stime.Unix()), val.Amount, val.Sign})
		}
		index++
	}
	encoded := make([][]byte,totalSeg)
	for i := 0; i < totalSeg; i++{

		toReport := ReceiptsOfReport{
			1,
			rs.account,
			receiptsArrays[i],
		}
		encoded[i],_ = rlp.EncodeToBytes(toReport)

		h := rlpHash(encoded[i])
		sig, err := crypto.Sign(h[:], rs.prvKey)
		if err == nil {
			encoded[i] = append(sig, encoded[i]...)
		}

	}
	id := crypto.PubkeyToAddress(rs.prvKey.PublicKey)
	if id != rs.account {
		err = ErrInvalidNode
	}
	result = encoded
	return

}
func (rs *ReceiptStore) SendDataToServer(url string, timeout time.Duration, result []byte) error {

	client := &http.Client{
		Timeout: timeout,
	}

	request, err := http.NewRequest("POST", url, bytes.NewReader(result))
	if err != nil {
		log.Error("error in post receipts", "reason", err)
	}
	request.Header.Set("Connection", "Keep-Alive")
	request.Header.Set("Content-Type", "text/plain")

	res, err := client.Do(request)
	if err == nil { //提交成功，本地删除

		defer res.Body.Close()
		if res.StatusCode == 200 {
			return nil
		} else {
			log.Error("error in post receipts", "status", res.Status, "code", res.StatusCode)
			return errors.New("status")
		}
	}
	return err
}
func (rs *ReceiptStore) doAutoSubmit() (error,int) {

	log.Info("report receipts to server 1")
	receipts,totalCnt := rs.GetReceiptsToReport()

	results, err := rs.createReportData(receipts)

	for i := 0; i < len(results); i ++ {
		timeout := time.Duration(60 * time.Second) //超时时间50ms
		log.Info("report receipts to server", "total amount", receipts.Amount(),"items:",totalCnt)
		err = util.SendDataToServers(rs.servers,ReportRoute, timeout, results[i])
		rs.hmu.Lock()

		len := len(rs.receiptsLogs)
		if len > 1000 {
			rs.receiptsLogs = rs.receiptsLogs[len-1000:]
		}
		rs.hmu.Unlock()
		if err != nil {
			break;
		}
	}
	rs.hmu.Lock()
	defer rs.hmu.Unlock()
	log.Info("report end with error", "error", err)
	if err == nil { //提交成功，本地删除
		rs.receiptsLogs = append(rs.receiptsLogs, receipts)
		rs.saveReceipts(RPREF, Receipts{})
	} else {
		//提交失败，本地已经存储过了
		rs.saveReceipts(RPREF, receipts)
	}
	return err,totalCnt
}
func (rs *ReceiptStore) mockAutoSubmit() error {
	result, err := rs.createReportData(rs.allReceipts)

	ioutil.WriteFile("./reportData", result[0], 0644)

	url := "http://127.0.0.1:8088/receipts"
	timeout := time.Duration(50 * time.Millisecond) //超时时间50ms
	err = rs.SendDataToServer(url, timeout, result[0])

	return err
}
func (rs *ReceiptStore) submitRoutine() {
	log.Info("Setup report subroutine ", "time", MAX_STIME_DURATION)
	rs.doAutoSubmit()
	timer := time.NewTimer(MAX_STIME_DURATION)
	for {
		select {
		case <-timer.C:

			rs.doAutoSubmit()
			timer.Reset(MAX_STIME_DURATION)
			case <-rs.quit:
				timer.Stop()
				return

		}
	}
}

type BalanceInfoDb struct {
	Second  int64
	Balance int64
}
type BalaceMessage struct {
	Balance  string `json:"balance"`
	VBalance string `json:"vBalance"`
}

const InvalidBalance = 0xFFFFFFFFFFFFFFF

//检查余额，以10^-12次方为单位
func (rs *ReceiptStore) CheckBalance(nodeId [20]byte) int64 {
	//如果不需要checkbalace，返回10个EUS，总是认为是有效的
	if !rs.checkBalance {
		return 10000000000000
	}
	hexId := common.Bytes2Hex(nodeId[:])
	dataRaw, exist := rs.balances.Get(hexId)
	var err error
	var data []byte
	if !exist {
		data, err = rs.db.Get([]byte(BALNACE_PREFIX+hexId), nil)
	} else {
		data = dataRaw.([]byte)
	}

	if err == nil {
		var valInDb BalanceInfoDb
		json.Unmarshal(data, &valInDb)
		//数据库中的未超时
		if time.Since(time.Unix(valInDb.Second, 0)) < MAX_STIME_DURATION {
			save, _ := json.Marshal(valInDb)
			rs.balances.Add(hexId, save)
			return int64(valInDb.Balance)
		}
	}
	//Get balance from server
	for _,server := range rs.servers {
		data, err = util.GetDataFromServer(server + AccountRoute + hexId)
		if err == nil {
			var m BalaceMessage
			err = json.Unmarshal(data, &m)
			if err != nil {
				Balance := int64(InvalidBalance)
				if m.VBalance != "" {
					balance, err := strconv.ParseFloat(m.VBalance, 64)
					if err != nil {
						Balance = int64(balance * 10000000000)
					}

				} else {
					balance, err := strconv.ParseFloat(m.Balance, 64)
					if err != nil {
						Balance = int64(balance * 10000000000)
					}
				}
				if Balance != InvalidBalance {
					dataToStore := BalanceInfoDb{time.Now().Unix(), Balance}
					result, err := json.Marshal(dataToStore)
					if err == nil {
						rs.db.Put([]byte(BALNACE_PREFIX+hexId), result, nil)
						rs.balances.Add(hexId, result)
					}
				}

				return Balance
			} else {
				return InvalidBalance
			}

		} else {

		}
	}
	log.Error("error in get balance", "reason", err)
	return InvalidBalance


}

/**
	每次数据传输完成后，用这个通知ReceiptStore，用于计数某些节点发送的总Chunk和收到的收据
    本函数返回一个unpayed的值，用于表示该节点目前有多少个未支付（收据）的数据了
	调用者可以根据这个返回决定对相应节点的操作

	TODO 根据unpaidAmount的设计黑名单
*/
func (rs *ReceiptStore) OnChunkDelivered(nodeId [20]byte, amount4k uint32) uint32 {
	rs.hmu.Lock()
	defer rs.hmu.Unlock()

	_, ok := rs.unpaidAmount[nodeId]
	if !ok {
		rs.unpaidAmount[nodeId] = amount4k
	} else {
		rs.unpaidAmount[nodeId] += amount4k
	}
	go rs.CheckBalance(nodeId)
	return rs.unpaidAmount[nodeId]
}

func (rs *ReceiptStore) decreaseOnNewReceipt(account [20]byte, count uint32) {
	_, ok := rs.unpaidAmount[account]
	if !ok {
		rs.unpaidAmount[account] = 0
	}
	if rs.unpaidAmount[account] > count {
		rs.unpaidAmount[account] -= count
	} else {
		rs.unpaidAmount[account] = 0
	}
}
