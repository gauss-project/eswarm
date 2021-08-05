package util

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"testing"
)

func _TestCipher(t *testing.T) {

	rawData := DefsV2{[]string{"enode://f4a9712067c8204af740c0596b5b5cef974bba52d1c4d9240974d51ffa9bd6caedf694c496ad66208e33a8b611edba57ee5ff30531b810c43c7dee5712da34c5@134.175.166.163:30396?nt=9","enode://5d95d2000514edd945c6ddf2a0bad86e058c24b938073de6c931bb0257351551a3cd29d1a2c92fe4a9af7f2bfc19cb111ec415b7f173b9b0dda547280c86de36@36.153.93.250:30399?nt=36","enode://7aa903cb306852357b41c1587d2df65651693ab8fcd6ffad512d3a3812df8a680c87322975f92a900dda7e81329fa43a05ce4218bcf0ec43fd0fe366e7e46a0b@39.165.44.145:5939?nt=36","enode://da936161f92d55bf6878b4533051936916a37ef866232ac43d1ca4e7e5d0e0d57511f379e137ca08f22a00412030cc5f302427352538c576f1fa5a584a9c909f@39.165.44.131:8602?nt=36"}, []string{"https://service.371738.com/apis/v1"}}
	//rawData := DefsV2{[]string{"enode://d8578bfef5b8e447d9e3ca674597b317a8ceb103da129a978d93eed61888e4cfaab19794e3c2e274a88de3ce238ae70fd5556f0365f323abbea2011f605861d6@172.16.1.11:30396"}, []string{"http://172.16.1.25:4000/apis/v1","http://172.16.1.24:4000/apis/v1"}}
	//rawData := Defs{[]string{"enode://8f84bcad710281f20a41e0faff94ae38044a764ec3182fde705ddb9ff5b166684a87bc6e53e227a5a735b6f5fb4e9d97867e20c66ebe069b87694151326bb9d0@174.128.233.42:30396?nt=9","enode://0e4aa108144d8678f82306985bf71ab65004bc248f2f239b9fc2085b7daaea8ee317d3178664b04e7104e6a7840a5c61e2f89cf9603ef11e4b5006b5c91dd46d@110.185.107.116:30400?nt=9","enode://644fb6626b80a423371a3b8ac044210c9ce56afc0effc6138bbf85447bc9311f14268974f287dc9a9b575c2b6211f2d8b273ee3580c75b8aceea59c7e4cead7c@23.237.178.90:30396?nt=9"}, "https://service.cdscfund.org/apis/v1"}

	result := CiphData(rawData)

		// open output file
		fo, err := os.Create("nodeslist.txt")
		if err != nil {
			panic(err)
		}
		// close fo on exit and check for its returned error
		defer func() {
			if err := fo.Close(); err != nil {
				panic(err)
			}
		}()

		if _, err := fo.WriteString(result); err != nil {
			panic(err)
		}

		def, err := DecipherData(result)
		if err == nil {
			t.Log("result:", def)
	} else {
		t.Error("error in cipher/decipher", err)
	}
}





const P = 0.30
type CacheInfo struct {
	key string
	value float64
}

func GetValue(m,n float64,info map[string]float64) float64{
	result := 0.0
	key := fmt.Sprintf("%v/%v",int32(m),int32(n))
	ret,exit := info[key]
	if exit  {
		return ret
	}
	if(m <=n ){
		result =  math.Pow(P,n)
	}else if(n==0) {
		result =  math.Pow(1-P,m)
	}else if(m == 1){
		result =  math.Pow(P,n)
	}else {
		result =  P*GetValue(m-1,n-1,info)+(1-P)*GetValue(m-1,n,info)
	}
	info[key] = result
	return result
}
func _TestCipherOut(t *testing.T) {
	info := make(map[string]float64)
	result := 0.0;
	for i := 0; i < 10;i++{
		result = result + GetValue(float64(10),float64(i),info)
		//0.9999999998941209  48 :0.9999999674198063
		//					  	    9999999561515294
		//0.9999999997691102
		//0.9999999999991741
		//0.9999980846217624
		//0.999999999999991
		//0.9999999999999913
	}
	fmt.Println(fmt.Sprintf("total :%v",result ))

}
func  Mutate(i interface{}, mutator interface{}) error {
	return mutate(i, cborMutator(mutator))
}
func mutate(i interface{}, mutator func([]byte) ([]byte, error)) error {


	mutated, err := mutator(reflect.ValueOf(i).Bytes())
	fmt.Print(mutated)
	if err != nil {
		return err
	}
	return nil


}

func cborMutator(mutator interface{}) func([]byte) ([]byte, error) {
	rmut := reflect.ValueOf(mutator)

	return func(in []byte) ([]byte, error) {
		state := reflect.New(rmut.Type().In(0).Elem())

		out := rmut.Call([]reflect.Value{state})

		if err := out[0].Interface(); err != nil {
			return nil, err.(error)
		}

		return []byte{0x01,0x02},nil
	}
}
func __TestValueOf(t *testing.T){

	err := Mutate(1, func(s string) error {
		return nil
	})

	fmt.Print(err)
}

type type1 struct {

	Field1 int
}

type type2 struct {
	type1
	Field2 *int
}

func TestStruct(t *testing.T){
	val :=type1{1}
	bytes,_ :=json.Marshal(val)
	var  val2 type2
	err2 := json.Unmarshal(bytes,val2)

	if err2 != nil{
		fmt.Println("error",err2)
	}else{
		fmt.Println(fmt.Sprintf("t2:%v",val2))
	}
}