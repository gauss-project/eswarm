package main

import (
	"fmt"
	"github.com/gauss-project/eswarm/mobile_swarm"
	"time"
)

func main() {

	path := "/Users/sean/Documents/test"
	//bootnode := "enode://156de950834f68902c2029a7efbf3f653e3ac924da0b4848819f61203dcd5aa774838e6d66df8da6108c1a1462cfdaaf0645ab9849c29ffd5641739684551457@172.16.1.11:30398"
	//bootnode := "enode://4b0581175812b8c15aa43f148f61af9c8a0d903cc5a26249e7400cda9ac6f0db35b4dbdf2423702fd693a0c6b75c05209709273379363d6fe742b70ec58bfd97@124.156.115.14:30399"
	//bootnode := "enode://e7f6cffab1841d4fdc118f1e21947b6d71cb29f663340fb817a266c3d42a2af9a0d350b247c619e64c528b97932b3f14e7f60643d4db232491c72a22a9d9b04a@110.185.107.116:30400"
	//err := eswarm.Clean(path, 1)
	//if err != nil { return }

	res, err := csdc.ActivateR(path, "5ca31612427e9a564c2089c2", "123", "123", "http://localhost:4000/apis/v1/activate", false, "123", 1)
	if err != nil {
		fmt.Println(res, err)
		//return
	}
	fmt.Println(res)

	node, err := csdc.StartL(path, "123", res.NodeUrl, res.BootNode, 4)

	fmt.Println(node.GetHttpPort())

	fmt.Println(node.GetM3U8BaseUrl())

	fmt.Println(node.GetM3U8Url("https://v.152878.com/vback/qYq5rD7Jq19gE/2358/0981B7353D085CA0AE868EA1DEE43067/0981B7353D085CA0AE868EA1DEE43067_ea6399f6616d4a60aa42487c2814e4c2.mp4/index.m3u8?token=cz05OWViYWFkZDI2MjI5NjBlOTM2ZTJjN2ZiMTQ2Nzg4ZiZpPTk5OTk5JnA9NSZ0PTE1NjUwOTIyOTImbj0wOTgxQjczNTNEMDg1Q0EwQUU4NjhFQTFERUU0MzA2N19lYTYzOTlmNjYxNmQ0YTYwYWE0MjQ4N2MyODE0ZTRjMi5tcDQvaW5kZXgubTN1OCZyPTEmdj0yLjAuMCZjPTImZT03MjA=", "00d534e271ae532aaaf05ff5a36e5e48b6193f7c8c6784c7a38653f462399243"))

	fmt.Println(node.GetFileBaseUrl())

	fmt.Println(node.GetFileUrl("http://mediacallback-test.potato.im/download/a8e186a234daa0d721d93885b0c8c000e17d85b3/ZpBiLd7B9233422815956410007", "950e4852cca684f48c18ed9c3368327cdb7731bb51d24c1280de120a5db258e2"))

	//timer := time.NewTimer(5 * time.Second)
	//<-timer.C
	//node.Stop()
	//timer = time.NewTimer(2 * time.Second)
	//<-timer.C
	//node.Start()
	if err != nil {
		fmt.Println(err)
		return
	}

	time.Sleep(3600 * time.Second)

}
