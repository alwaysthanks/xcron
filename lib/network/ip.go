package network

import (
	"errors"
	"net"
)

var (
	errGetInternalIpFailed   = errors.New("get internal ip failed")
	errEthIntranetIpNotExist = errors.New("eth internal ip not exist")
)

func GetDefaultInternalIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errGetInternalIpFailed
}

func GetEthInternalIp(eth ...string) (string, error) {
	ips := make(map[string]net.Addr)
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, i := range interfaces {
		byName, err := net.InterfaceByName(i.Name)
		if err != nil {
			return "", err
		}
		addrs, err := byName.Addrs()
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if ok && ipnet.IP.To4() != nil {
				if len(eth) == 0 && !ipnet.IP.IsLoopback() {
					//default
					return ipnet.IP.String(), nil
				}
				ips[byName.Name] = addr
			}
		}
	}
	if len(eth) == 0 {
		return "", errGetInternalIpFailed
	}
	if addr, ok := ips[eth[0]]; ok {
		ipnet := addr.(*net.IPNet)
		return ipnet.IP.String(), nil
	}
	return "", errEthIntranetIpNotExist
}
