package state

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"github.com/gauss-project/eswarm/common"
	"github.com/gauss-project/eswarm/crypto"
	"github.com/gauss-project/eswarm/log"
	"github.com/gauss-project/eswarm/p2p/enode"
	"github.com/gauss-project/eswarm/rlp"
	"github.com/syndtr/goleveldb/leveldb"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"
)

/**
	//函数测试
	    持久化存储/恢复
		GetReceiptsToReport

	//有效的签名
	生成签名=>验证签名


	//非法签名：
    1。不正确的nodeId
    2。不正确的签名
    3. 不正确的时间

	//流程
	1。连续增加的收据
    2。混乱的收据   12534
	3。多个STIME混合

	4。接收多个节点的数据


	//故障
	1。断开/恢复
    2。缺收据
    3。多收据
*/

func getOrCreateKey(number int) *ecdsa.PrivateKey {
	keyFile := fmt.Sprintf("private%v", number)
	prvKey, err := crypto.LoadECDSA(keyFile)
	if err != nil {
		prvKey, err = crypto.GenerateKey()
		if err == nil {
			crypto.SaveECDSA(keyFile, prvKey)
		}
	}
	return prvKey

}

func createReceiptStore(t *testing.T, index int) *ReceiptStore {
	rand.Seed(time.Now().Unix())

	fileName := fmt.Sprintf("db_store_test%v", index)
	dir := "tmpDir/" // err := ioutil.TempDir("", "db_store_test")

	db, err := leveldb.OpenFile(dir+fileName, nil)
	if err != nil {
		t.Error(err)
	}
	receiptStore := newReceiptsStore(db, getOrCreateKey(index), "",false,false)

	return receiptStore
}

func testReceipt(receipt Receipt) (common.Address, error) {
	pubKey, isOk := receipt.Verify()
	if !isOk {
		return common.Address{0}, ErrInvalidSignature
	}
	//根据这个pubKey生成nodeId
	nodeId := crypto.PubkeyToAddress(*pubKey)
	return nodeId, nil
}

func Test_SpecSign(t *testing.T) {

	timeSecond := []byte{92, 163, 115, 63}
	b_buf := bytes.NewBuffer(timeSecond)
	var x uint32
	binary.Read(b_buf, binary.BigEndian, &x)

	amount_ := []byte{0, 0, 29, 132}
	amount_buf := bytes.NewBuffer(amount_)
	var amount uint32
	binary.Read(amount_buf, binary.BigEndian, &amount)

	aReceipt := Receipt{ReceiptBody{common.HexToAddress("3b2c32a3e1da99a3037fcd80921b1cb925cecdfb"), time.Unix(int64(x), int64(0)), amount}, []byte{70, 39, 232, 77, 177, 223, 35, 192, 239, 239, 231, 223, 112, 1, 107, 87, 127, 237, 97, 183, 170, 250, 12, 37, 131, 177, 111, 225, 55, 180, 11, 19, 50, 121, 33, 60, 161, 31, 17, 98, 110, 11, 117, 53, 226, 224, 94, 231, 62, 129, 57, 125, 27, 21, 5, 40, 116, 109, 101, 138, 192, 207, 223, 3, 0}}

	nodeId, err := testReceipt(aReceipt)
	if err == nil {
		t.Log("Verify Ok:", "nodeId", nodeId)
	} else {
		t.Error("errror in verify:", "reason", err)
	}

}
func Test_SignAny(t *testing.T) {
	vkey, _ := crypto.GenerateKey()
	hash := [32]byte{0x01}
	for i := 0; i < 32; i++ {
		hash[i] = byte(i)
	}

	sign, err := crypto.Sign(hash[:], vkey)
	if err == nil {
		hash[0] = 0x70
		pbkey, err := crypto.Ecrecover(hash[:], sign)
		if err == nil {
			result := crypto.VerifySignature(pbkey, hash[:], sign[0:64])
			if result {
				t.Log("any signature")
				return
			}
		}
	}
	t.Log("any signature not passed")

}
func Test_ReceiptRlp(t *testing.T) {
	// atime := time.Unix(0,big.NewInt(time.Now().UnixNano()).Int64())
	// t.Log(atime.Format("2006-01-02 15:04:01"))
	key := getOrCreateKey(0)
	nodeId := crypto.PubkeyToAddress(key.PublicKey)
	data := ReceiptBody{nodeId, time.Now(), 1}

	bytes, err := rlp.EncodeToBytes(data)
	if err == nil {
		newBody := new(ReceiptBody)
		rlp.DecodeBytes(bytes, newBody)

		difference := newBody.Stime.Sub(data.Stime)

		if difference < 1*time.Second {
			t.Log("Encode/Decode ReceiptBody OK")
			return
		}
	}
	t.Error("Encode/Decode ReceiptBody Error")
}

func Test_Signature(t *testing.T) {
	pvKeySigner := getOrCreateKey(2)
	pvKeyProvider := getOrCreateKey(3)
	node2 := crypto.PubkeyToAddress(pvKeySigner.PublicKey)
	node3 := crypto.PubkeyToAddress(pvKeyProvider.PublicKey)
	anReceipt := &Receipt{ReceiptBody{node3, time.Now(), 155}, []byte{}}
	anReceipt.Signature(pvKeySigner)

	signerKey, ok := anReceipt.Verify()
	if ok {
		if node2 == crypto.PubkeyToAddress(*signerKey) {
			t.Log("Check signature and extract nodeId ok")
		} else {
			t.Error("Check signature failed, node Id error")
		}
	} else {
		t.Error("Check signature failed, verify key failed")
	}

}

func cloneStore(store *ReceiptStore) *ReceiptStore {
	receiptStore := newReceiptsStore(store.db, store.prvKey, "",false)

	return receiptStore
}

var store1, store2, store3, store4, store5, store6 *ReceiptStore

func PrepareForTest(t *testing.T) {

	os.RemoveAll("tmpDir")

	store1 = createReceiptStore(t, 0)
	store2 = createReceiptStore(t, 1)
	store3 = createReceiptStore(t, 2)
	store4 = createReceiptStore(t, 3)
	store5 = createReceiptStore(t, 4)
	store6 = createReceiptStore(t, 5)
}
func Test_CRecStream(t *testing.T) {
	PrepareForTest(t)
	//3 from store2
	store1.OnNodeChunkReceived(store2.account, 4096)
	store1.OnNodeChunkReceived(store2.account, 4096)
	store1.OnNodeChunkReceived(store2.account, 4096)

	//4 from store3
	store1.OnNodeChunkReceived(store3.account, 4096)
	store1.OnNodeChunkReceived(store3.account, 4096)
	store1.OnNodeChunkReceived(store3.account, 4096)
	store1.OnNodeChunkReceived(store3.account, 4096)

	//1 from store4
	store1.OnNodeChunkReceived(store4.account, 4096)

	//2 from store5
	store1.OnNodeChunkReceived(store5.account, 4096)
	store1.OnNodeChunkReceived(store5.account, 4096)

	//7 from store6
	for i := 0; i < 7; i++ {
		store1.OnNodeChunkReceived(store6.account, 4096)
	}

	timerC := time.NewTimer(1 * time.Second)
	QuitC := make(chan int)
	go func() {
		select {
		case <-timerC.C:
			isOk := false
			storeTest := cloneStore(store1)
			keys := storeTest.nodeCommCache.Keys()
			if len(keys) == 5 {
				r2, ok2 := storeTest.nodeCommCache.Get(store2.account)
				r3, ok3 := storeTest.nodeCommCache.Get(store3.account)
				r4, ok4 := storeTest.nodeCommCache.Get(store4.account)
				r5, ok5 := storeTest.nodeCommCache.Get(store5.account)
				r6, ok6 := storeTest.nodeCommCache.Get(store6.account)

				if ok2 && ok3 && ok4 && ok5 && ok6 {
					if r2.(*ReceiptInStore).Amount == 3 && r3.(*ReceiptInStore).Amount == 4 && r4.(*ReceiptInStore).Amount == 1 &&
						r5.(*ReceiptInStore).Amount == 2 && r6.(*ReceiptInStore).Amount == 7 {
						isOk = true
					}
				} else {
					t.Error("read error")
				}
			}
			if isOk {
				QuitC <- 0
			} else {
				QuitC <- 1
			}
		}

	}()

	result := <-QuitC

	if result != 0 {
		t.Error("C Record create save reload failed")
	} else {
		t.Log("C Record create save reload passed")
	}
}

func mockChunkDelivery(chunkSender, chunkReceiver *ReceiptStore, t *testing.T) {

	receipt, err := chunkReceiver.OnNodeChunkReceived(chunkSender.account, 4096)
	if err != nil {
		t.Error(err)
	}
	chunkSender.OnNewReceipt(enode.ID{},receipt)
}
func Test_HRecStream(t *testing.T) {

	for i := 0; i < 3; i++ {
		mockChunkDelivery(store1, store2, t)
	}
	for i := 0; i < 5; i++ {
		mockChunkDelivery(store1, store3, t)
	}
	for i := 0; i < 10; i++ {
		mockChunkDelivery(store1, store4, t)
	}
	for i := 0; i < 11; i++ {
		mockChunkDelivery(store1, store5, t)
	}
	for i := 0; i < 1; i++ {
		mockChunkDelivery(store1, store6, t)
	}
	timerC := time.NewTimer(1 * time.Second)
	QuitC := make(chan int)
	go func() {
		select {
		case <-timerC.C:
			isOk := false
			storeTest := cloneStore(store1)
			receipts := storeTest.allReceipts
			if len(receipts) == 5 {
				r2, ok2 := receipts[store2.account]
				r3, ok3 := receipts[store3.account]
				r4, ok4 := receipts[store4.account]
				r5, ok5 := receipts[store5.account]
				r6, ok6 := receipts[store6.account]

				if ok2 && ok3 && ok4 && ok5 && ok6 {
					if len(r2) == 1 && len(r3) == 1 && len(r4) == 1 && len(r5) == 1 && len(r6) == 1 {
						isOk = true
					}
				} else {
					t.Error("read error")
				}
			}
			if isOk {
				QuitC <- 0
			} else {
				QuitC <- 1
			}
		}

	}()

	result := <-QuitC

	if result != 0 {
		t.Error("H Record create save reload failed")
	} else {
		t.Log("H Record create save reload passed")
	}
}
func Test_InvalidSignature(t *testing.T) {

	pvKeySigner := getOrCreateKey(2)
	pvKeyProvider := getOrCreateKey(3)
	//node2 := enode.PubkeyToIDV4(&pvKeySigner.PublicKey)
	node3 := crypto.PubkeyToAddress(pvKeyProvider.PublicKey)
	invNodeIdReceipt := &Receipt{ReceiptBody{node3, time.Now(), 155}, []byte{}}
	invNodeIdReceipt.Signature(pvKeySigner)

	InvalidTimeReceipt := &Receipt{ReceiptBody{store1.account, time.Now().Add(121 * time.Minute), 155}, []byte{}}
	InvalidTimeReceipt.Signature(pvKeySigner)

	InvalidTimeReceipt2 := &Receipt{ReceiptBody{store1.account, time.Now().Add(-121 * time.Minute), 155}, []byte{}}
	InvalidTimeReceipt2.Signature(pvKeySigner)

	InvalidSignReceipt := &Receipt{ReceiptBody{store1.account, time.Now().Add(-121 * time.Minute), 155}, []byte{}}
	InvalidSignReceipt.Signature(pvKeySigner)
	InvalidSignReceipt.Sign[0] += InvalidSignReceipt.Sign[0]
	if InvalidSignReceipt.Sign[0] == 0 {
		InvalidSignReceipt.Sign[0] = 1
	}

	if store1.OnNewReceipt(enode.ID{},invNodeIdReceipt) != ErrInvalidNode {
		t.Error("Invalid signature error invalid node undetected")
		return
	}
	if store1.OnNewReceipt(enode.ID{},InvalidTimeReceipt) != ErrInvalidSTime {
		t.Error("Invalid signature error invalid postponed time undetected")
		return
	}
	if store1.OnNewReceipt(enode.ID{},InvalidTimeReceipt2) != ErrInvalidSTime {
		t.Error("Invalid signature error invalid advanced time undetected")
		return
	}
	if store1.OnNewReceipt(enode.ID{},InvalidSignReceipt) != ErrInvalidSTime {
		t.Error("Invalid signature error invalid signature  undetected")
		return
	}

	t.Log("Invalid signature test passed")
}

func Test_DeliverCounter(t *testing.T) {
	var receipt *Receipt
	unpaied := uint32(10)
	for i := uint32(0); i < unpaied; i++ {
		store4.OnChunkDelivered(store5.account, 4096)
		receipt, _ = store5.OnNodeChunkReceived(store4.account, 4096)
	}
	calcUnpaied, _ := store4.unpaidAmount[store5.account]
	if calcUnpaied != unpaied {
		t.Error("Test deliver counter failed: unpaiedAmount error")
		return
	}

	store4.OnNewReceipt(enode.ID{},receipt)

	calcUnpaied, _ = store4.unpaidAmount[store5.account]
	if calcUnpaied != 0 {
		t.Error("Test deliver counter failed: unpaiedAmount not reset")
		return
	}
	t.Log("Test deliver counter passed")
}
func Shuffle(vals []*Receipt) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	// We start at the end of the slice, inserting our random
	// values one at a time.
	for n := len(vals); n > 0; n-- {
		randIndex := r.Intn(n)
		// We swap the value at index n-1 and the random index
		// to move our randomly chosen value to the end of the
		// slice, and to move the value that was at n-1 into our
		// unshuffled portion of the slice.
		vals[n-1], vals[randIndex] = vals[randIndex], vals[n-1]
	}
}
func Test_Unordered(t *testing.T) {
	receipts := make([]*Receipt, 20)
	unpaied := uint32(20)
	for i := uint32(0); i < unpaied; i++ {
		store4.OnChunkDelivered(store5.account, 4096)
		receipt, _ := store5.OnNodeChunkReceived(store4.account, 4096)
		receipts[i] = receipt
	}
	calcUnpaied, _ := store4.unpaidAmount[store5.account]
	if calcUnpaied != unpaied {
		t.Error("Test deliver counter failed: unpaiedAmount error")
		return
	}
	Shuffle(receipts)

	for i := uint32(0); i < unpaied; i++ {
		store4.OnNewReceipt(enode.ID{},receipts[i])
	}

	calcUnpaied, _ = store4.unpaidAmount[store5.account]
	if calcUnpaied != 0 {
		t.Error("Test unordered receipts failed: unpaiedAmount not reset")
		return
	}
	t.Log("Test  unordered receipts passed")
}

func createSpecifiyReceipt(providerIndex int, receiverIndex int, singTime time.Time, amount uint32) *Receipt {
	pvKeySigner := getOrCreateKey(receiverIndex)
	pvKeyProvider := getOrCreateKey(providerIndex)

	node3 := crypto.PubkeyToAddress(pvKeyProvider.PublicKey)
	anReceipt := &Receipt{ReceiptBody{node3, singTime, amount}, []byte{}}
	anReceipt.Signature(pvKeySigner)
	return anReceipt
}
func Test_MultiStime(t *testing.T) {
	receipts := make([]*Receipt, 0)
	for i := 0; i < 10; i++ {
		stime := time.Now().Add(5 * time.Minute)
		for j := 0; j < 20; j++ {
			receipts = append(receipts, createSpecifiyReceipt(6, 5, stime, uint32(j)))
		}
	}
	Shuffle(receipts)
	for _, receipt := range receipts {
		store6.OnNewReceipt(enode.ID{},receipt)
	}
	failed := false
	if len(store6.allReceipts) == 1 {
		for _, receiptsOfNode := range store6.allReceipts {
			if len(receiptsOfNode) == 10 {
				for _, receiptItem := range receiptsOfNode {
					if receiptItem.Amount != 20 {
						failed = true
						break
					}
				}
			}
			if failed {
				break
			}
		}
	}

	if failed {
		t.Error("Test multi stime  failed")
		return
	}
	t.Log("Test multi stime  passed")
}

func Test_Submit(t *testing.T) {
	store4.mockAutoSubmit()
}

func Test_Unordered2(t *testing.T) {
	receipts := make([]*Receipt, 20)
	unpaied := uint32(20)
	for i := uint32(0); i < unpaied; i++ {
		store4.OnChunkDelivered(store5.account, 4096)
		receipt, _ := store5.OnNodeChunkReceived(store4.account, 4096)
		receipts[i] = receipt
	}
	calcUnpaied, _ := store4.unpaidAmount[store5.account]
	if calcUnpaied != unpaied {
		t.Error("Test deliver counter failed: unpaiedAmount error")
		return
	}
	Shuffle(receipts)

	for i := uint32(0); i < unpaied; i++ {
		store4.OnNewReceipt(enode.ID{},receipts[i])
	}

	calcUnpaied, _ = store4.unpaidAmount[store5.account]
	if calcUnpaied != 0 {
		t.Error("Test unordered receipts failed: unpaiedAmount not reset")
		return
	}
	t.Log("Test  unordered receipts passed")
	store4.mockAutoSubmit()
}

func loadReceiptStore(t *testing.T, dbPath string) *ReceiptStore {

	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		panic(err)
	}
	receiptStore := newReceiptsStore(db, getOrCreateKey(100), "http://192.168.1.10:4000/apis/v1",false)

	return receiptStore
}

func Test_ReadExternData(t *testing.T) {

	rs := loadReceiptStore(t,"/Users/aegon/Downloads/Potato Desktop/receipts.db 2")
	receipts,_ := rs.GetReceiptsToReport()
	result, err := rs.createReportData(receipts)
	if err == nil {
		resultLen := len(result)
		log.Info("receipts is:",receipts,"len",resultLen)
	}

	tm := time.NewTicker(5*time.Millisecond)

	for i := range tm.C {
		log.Info("ok","i",i)
	}



}

func  Test_Post(t *testing.T)  {

	client := &http.Client{
		Timeout: 5*time.Second,
	}

	request, err := http.NewRequest("POST", "http://127.0.0.1:3000", bytes.NewReader([]byte("{}")))
	if err != nil {
		log.Error("error in post receipts","reason",err)
	}
	//request.Header.Set("Connection", "Keep-Alive")
	//request.Header.Set("Content-Type", "text/plain")

	res, err := client.Do(request)
	if err == nil { //提交成功，本地删除

		defer res.Body.Close()
		if res.StatusCode == 200 {
			//return nil
		}else {
			log.Error("error in post receipts","status",res.Status,"code",res.StatusCode)
			//return errors.New("status")
		}
	}
	//return err

}