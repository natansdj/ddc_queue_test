package main

import (
	"fmt"
	"github.com/astaxie/beego"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	PrmHttpT     *http.Transport
	PrmNetClient *http.Client
	once         sync.Once
)

func startLoadTest2() {
	count := 0
	for {
		resp, err := PrmNetClient.Get("http://localhost:8085/v3/promoProduct/disc/2122")
		//resp, err := PrmNetClient.Get("http://localhost:8085/v3/promoProduct/bigi/2122")
		//resp, err := PrmNetClient.Get("http://localhost:8085/v3/branch")
		//resp, err := PrmNetClient.Get("http://localhost:8085/v3/orderPromo/test")
		//resp, err := PrmNetClient.Get("http://localhost:8085/v3/product")
		if err != nil {
			fmt.Println("INI ERROR : ", err.Error())
		}
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		log.Printf("Finished GET order request Response #%v", count)
		count += 1
	}
}

func startLoadTest1() {
	count := 0
	for {
		resp, err := PrmNetClient.Get("http://localhost:8084/v3/promoTest/test")
		if err != nil {
			fmt.Println("INI ERROR : ", err.Error())
		}
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
		log.Printf("Finished GET local request Response #%v", count)
		count += 1
	}
}

func init() {
	PrmHttpT = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:       10 * time.Second,
			KeepAlive:     100 * time.Second,
			FallbackDelay: -1,
		}).DialContext,
		//ForceAttemptHTTP2:     true,
		MaxIdleConnsPerHost:   1024,
		MaxIdleConns:          1024,
		MaxConnsPerHost:       1024,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	PrmNetClient = &http.Client{
		Timeout:   time.Second * 2,
		Transport: PrmHttpT,
	}

	beego.Debug("init PromoRequest")
}

func main() {

	for i := 0; i < 100; i++ {
		startLoadTest2()
		//startLoadTest1()
	}

}
