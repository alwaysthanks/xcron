package network

import "testing"

func TestGetDefaultInternalIp(t *testing.T) {
	ip, err := GetDefaultInternalIp()
	if err != nil {
		t.Fatalf("get ip err:%s", err.Error())
	}
	t.Logf("ip:%v", ip)
}

func TestGetEthInternalIp(t *testing.T) {
	ip, err := GetEthInternalIp("eth3")
	if err != nil {
		t.Fatalf("get ip err:%s", err.Error())
	}
	t.Logf("ip:%v", ip)
}
