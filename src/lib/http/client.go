package http

import (
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type HttpClient struct {
	tryTimes int
	instance *http.Client
}

func NewHttpClient(tryTimes int, timeout time.Duration) *HttpClient {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     time.Second * 15,
		},
		Timeout: timeout,
	}
	return &HttpClient{tryTimes: tryTimes, instance: client}
}

func (client *HttpClient) Post(url string, body io.Reader) ([]byte, error) {
	var httpResp *http.Response
	defer func() {
		if httpResp != nil && httpResp.Body != nil {
			httpResp.Body.Close()
		}
	}()
	for i := 0; i < client.tryTimes; i++ {
		httpReq, err := http.NewRequest("POST", url, body)
		if err != nil {
			log.Println("[HttpClient.Post] NewRequest err:", err)
			continue
		}
		httpResp, err = client.instance.Do(httpReq)
		if err != nil {
			if httpResp != nil && httpResp.Body != nil {
				httpResp.Body.Close()
			}
			log.Println("[HttpClient.Post] Do err:", err)
			continue
		}
	}
	respBody, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	} else {
		return respBody, nil
	}
}
