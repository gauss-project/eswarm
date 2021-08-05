package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gauss-project/eswarm/common"
	"github.com/gauss-project/eswarm/log"
	"github.com/gauss-project/eswarm/rlp"
)

// createHTTPClient for connection re-use
func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			Proxy:           http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 300 * time.Second,
			}).DialContext,
			MaxIdleConns:        2,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     1000 * time.Second,
		},
	}
	return client
}

type HttpReader struct {
	httpClient *http.Client
	unreported ReportData
	totalLen   int64
	reportMu   sync.Mutex
	account    [20]byte
	reportUrls []string
}

func CreateHttpReader() *HttpReader {
	client := createHTTPClient
	return &HttpReader{
		httpClient: client(),
		unreported: make(ReportData),
		totalLen:   0,
	}
}
func (hr *HttpReader) SetCdnReporter(account string, serverUrls []string) {
	hr.account = common.HexToAddress(account)
	hr.reportUrls = serverUrls
}
func (hr *HttpReader) GetDataLenFromCenter() int64 {
	return hr.totalLen
}

//从中心化服务端取数据，最多取1024*1024*8 （8M字节数据）
const MaxLen = 25 * 1024 * 1024

func (hr *HttpReader) GetChunkFromCentral(uri string, start int64, topHash []byte, r *http.Request) (result []byte, isEnd bool) {
	req, err := http.NewRequest("GET", uri, bytes.NewBuffer([]byte("")))
	if err != nil {
		result = nil
		isEnd = true
		return
	}
	for k, vv := range r.Header {
		vv2 := make([]string, len(vv))
		copy(vv2, vv)
		req.Header[k] = vv2
	}

	req.Header["Range"] = []string{fmt.Sprintf("bytes=%v-%v", start, start+MaxLen)}
	// use httpClient to send request
	response, err := hr.httpClient.Do(req)
	if err == nil && response != nil {

		// Close the connection to reuse it
		defer response.Body.Close()

		// Let's check if the work actually is done
		// We have seen inconsistencies even when we get 200 OK response
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Error("Couldn't parse response body. %+v", err)
		}
		length := int64(len(body))
		if length > MaxLen {
			length = MaxLen
		}
		result = body
		contentRange := response.Header["Content-Range"]
		if len(contentRange) > 0 {
			patternM3u8 := regexp.MustCompile(`bytes\s*[0-9]+-(?P<end>[0-9]+)\/(?P<total>[0-9]+)`)
			matchResult := patternM3u8.FindSubmatch([]byte(contentRange[0]))
			if len(matchResult) > 2 {
				endByte, err1 := strconv.ParseInt(string(matchResult[1]), 10, 64)
				totalByte, err2 := strconv.ParseInt(string(matchResult[2]), 10, 64)
				if err1 == nil && err2 == nil && endByte+1 != totalByte {
					isEnd = false
				} else {
					isEnd = true
				}
			} else {
				isEnd = true
			}
			if len(hr.reportUrls) != 0 {
				hr.doReport(hr.reportUrls,"/cdn" ,topHash, int64(len(result)))
			}

		} else {
			isEnd = true
		}
		return
		//log.Trace("Response Body:", string(body))
	} else {
		result = nil
		isEnd = true
		return
	}
}

type OnError func(http.ResponseWriter, *http.Request, string, int)

//get data from server and write to response
func (s *HttpReader) GetDataFromCentralServer(uri string, r *http.Request, w http.ResponseWriter, hash []byte, onError OnError) (retrieved bool) {
	retrieved = true
	http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	req, err := http.NewRequest("GET", uri, bytes.NewBuffer([]byte("")))
	if err != nil {
		log.Error("Error Occured. %+v", err)
		onError(w, r, fmt.Sprintf("Error Occured :%+v", err), http.StatusInternalServerError)
		retrieved = false
		return
	}
	for k, vv := range r.Header {
		vv2 := make([]string, len(vv))
		copy(vv2, vv)
		req.Header[k] = vv2
	}

	// use httpClient to send request
	response, err := s.httpClient.Do(req)
	if err != nil || response == nil || (response.StatusCode < 200 || response.StatusCode >= 300) {
		log.Error("Error sending request to API endpoint.", "error:", err)
		retrieved = false
		onError(w, r, fmt.Sprintf("Error Occured :%+v", err), http.StatusBadRequest)
	} else {
		// Close the connection to reuse it
		defer response.Body.Close()

		// Let's check if the work actually is done
		// We have seen inconsistencies even when we get 200 OK response
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Error("Couldn't parse response body. %+v", err)
		}
		if len(s.reportUrls) != 0 {
			s.doReport(s.reportUrls,"/cdn", hash, int64(len(body)))
		}
		if w != nil {
			for k, vv := range response.Header {
				vv2 := make([]string, len(vv))
				copy(vv2, vv)
				if w.Header().Get(k) == "" {
					w.Header().Set(k, vv[0])
				}

				//	w.Header().Set(k,vv2)
				//log.Trace(k,vv)
			}
			w.Write(body)
		}
		retrieved = true

		//log.Trace("Response Body:", string(body))
	}
	return
}

type RpData struct {
	Stime   uint32
	MSec    uint32
	AmountH uint32
	AmountL uint32
}

type ReportData map[time.Time]int64

func (r *ReportData) EncodeRLP(w io.Writer) error {
	value := make([]RpData, 0)
	for stime, amount := range *r {
		stime := stime.UnixNano()
		sec := stime / 1e9
		msec := stime - sec
		data := RpData{uint32(sec), uint32(msec), uint32(amount >> 32), uint32(amount)}
		value = append(value, data)
	}
	return rlp.Encode(w, value)
}
func (rd *ReportData) DecodeRLP(s *rlp.Stream) error {
	value := make([]*RpData, 0)
	if err := s.Decode(&value); err != nil {
		return err
	}
	for _, res := range value {
		(*rd)[time.Unix(0, int64(res.Stime*1e9)+int64(res.MSec))] = (int64(res.AmountH) << 32) + int64(res.AmountL)
	}

	return nil
}

type ReportFmt struct {
	Account [20]byte
	Hash    []byte
	Data    []byte
}

func (r *HttpReader) doReport(urls []string, cmd string, hash []byte, dataLen int64) {
	r.reportMu.Lock()
	val := (dataLen + 4095) >> 12;
	r.unreported[time.Now()] = val
	r.totalLen += val
	r.reportMu.Unlock()
	go func() {
		r.reportMu.Lock()
		dataToReport := r.unreported
		r.unreported = make(ReportData)
		r.reportMu.Unlock()
		result, _ := rlp.EncodeToBytes(&dataToReport)
		data, _ := rlp.EncodeToBytes(ReportFmt{r.account, hash, result})
		err := SendDataToServers(urls,cmd, 5*time.Second, data)
		if err != nil {
			r.reportMu.Lock()
			for stime, amount := range dataToReport {
				r.unreported[stime] = amount
			}
			r.reportMu.Unlock()
		}
	}()
}
func SendDataToServers(hosts []string,cmd string, timeout time.Duration, data []byte) error {

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			Proxy:           http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 300 * time.Second,
			}).DialContext,
			MaxIdleConns:        2,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     1000 * time.Second,
		},
	}
	last_err := error(nil)
	for _,host := range hosts {
		url := host+cmd
		request, err := http.NewRequest("POST", url, bytes.NewReader(data))
		if err != nil {
			log.Info("error to send data", "reason", err)
		}else{
			log.Info("send report ok ","url",url)
		}
		request.Header.Set("Connection", "Keep-Alive")
		request.Header.Set("Content-Type", "text/plain")

		res, err := client.Do(request)
		last_err = err
		if err == nil { //提交成功，本地删除
			defer res.Body.Close()
			if res.StatusCode != 200 {
				log.Error("error response in send receipts", "error", res.StatusCode, "url", url)
				err = errors.New(res.Status)
			} else {
				break;
			}
		} else {
			log.Error("error in send receipts", "error", err, "url", url)
		}

	}

	return last_err
}

func GetDataFromServer(url string) (data []byte, err error) {
	resp, err := http.Get(url)
	if err != nil {
		// handle error
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
	} else {
		data = []byte(body)
	}

	return
}

type Defs struct {
	BootNodes  []string
	ReportAddr string
}
type DefsV2 struct {
	BootNodes  []string
	ReportAddrs []string
}

var commonIV = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}
var encryptKey = "Damn@CCCP&Descendants#2019#20200"

func GetBootnodesInfo(uri string) (bootnodes []string, reportAddress []string, err error) {

	req, err := http.NewRequest("GET", uri, bytes.NewBuffer([]byte("")))
	if err != nil {
		log.Error("Bootnode Error Occured. %+v", err)

		return []string{},  []string{}, err
	}
	client := createHTTPClient()
	// use httpClient to send request
	response, err := client.Do(req)
	if err != nil || response == nil || (response.StatusCode < 200 || response.StatusCode >= 300) {
		result := "";

		if err != nil {
			result = err.Error();
		}else if response == nil {
			result = "response is nil";
		}else {
			result = fmt.Sprintf("status code is %v",response.StatusCode)
		}

		log.Error("Error sending request to API endpoint", "error:", result)
		err = errors.New(result)
		return []string{}, []string{}, err
	} else {

		// Close the connection to reuse it
		defer response.Body.Close()

		// Let's check if the work actually is done
		// We have seen inconsistencies even when we get 200 OK response
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Error("bootnode error in get body %v",err)
			return []string{},  []string{}, err
		}

		//解码

		result, err := DecipherData(strings.Replace(string(body), "\n", "", -1))

		if err == nil {
			log.Info("Get :", "cnt", len(result.BootNodes))
			return result.BootNodes, result.ReportAddrs, nil
		} else {
			log.Error("bootnode error in decode body %v",err)
			return nil,  []string{}, err
		}

		//log.Trace("Response Body:", string(body))
	}
}

func DecipherData(cipherData string) (*DefsV2, error) {
	// 创建加密算法aes
	c, err := aes.NewCipher([]byte(encryptKey))
	if err != nil {
		//	fmt.Printf("Error: NewCipher(%d bytes) = %s", len(encryptKey), err)
		return nil, err
	}

	asBytes := common.FromHex(string(cipherData))
	// 解密字符串
	cfbdec := cipher.NewCFBDecrypter(c, commonIV)

	plaintextCopy := make([]byte, len(asBytes))
	cfbdec.XORKeyStream(plaintextCopy, asBytes)

	resultv1 := Defs{}
	result := DefsV2{}
	err = rlp.DecodeBytes(plaintextCopy, &resultv1)
	if err == nil {
		result.BootNodes = resultv1.BootNodes
		result.ReportAddrs = append([]string{},resultv1.ReportAddr)
	}else{
		err = rlp.DecodeBytes(plaintextCopy, &result)
	}



	//fmt.Printf("%x=>%s\n", asBytes, plaintextCopy)
	return &result, err
}
func CiphData(toCipher DefsV2) string {
	// 创建加密算法aes
	c, err := aes.NewCipher([]byte(encryptKey))
	if err != nil {
		//fmt.Printf("Error: NewCipher(%d bytes) = %s", len(encryptKey), err)
		return ""
	}
	data, _ := rlp.EncodeToBytes(toCipher)
	//加密字符串
	cfb := cipher.NewCFBEncrypter(c, commonIV)
	ciphertext := make([]byte, len(data))
	cfb.XORKeyStream(ciphertext, data)
	//fmt.Printf("%s=>%x\n", data, ciphertext)
	return common.Bytes2Hex(ciphertext)
}

/*
package main

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"os"
)



func main() {
	//需要去加密的字符串
	plaintext := []byte("My name is Astaxie")
	//如果传入加密串的话，plaint就是传入的字符串
	if len(os.Args) > 1 {
		plaintext = []byte(os.Args[1])
	}

	//aes的加密字符串
	key_text := "Damn@CCCP&Descendants#2019"
	if len(os.Args) > 2 {
		key_text = os.Args[2]
	}

	fmt.Println(len(key_text))

	// 创建加密算法aes
	c, err := aes.NewCipher([]byte(key_text))
	if err != nil {
		fmt.Printf("Error: NewCipher(%d bytes) = %s", len(key_text), err)
		os.Exit(-1)
	}

	//加密字符串
	cfb := cipher.NewCFBEncrypter(c, commonIV)
	ciphertext := make([]byte, len(plaintext))
	cfb.XORKeyStream(ciphertext, plaintext)
	fmt.Printf("%s=>%x\n", plaintext, ciphertext)

	// 解密字符串
	cfbdec := cipher.NewCFBDecrypter(c, commonIV)
	plaintextCopy := make([]byte, len(plaintext))
	cfbdec.XORKeyStream(plaintextCopy, ciphertext)
	fmt.Printf("%x=>%s\n", ciphertext, plaintextCopy)
}
*/
