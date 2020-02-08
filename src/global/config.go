package global

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
)

const (
	XcronPeersFile = "peers.json"
)

var XcronConf config

func InitConfig(filePath string) error {
	_, err := toml.DecodeFile(filePath, &XcronConf)
	if err != nil {
		return err
	}
	//fix peers
	peerFile := filepath.Join(XcronConf.RaftDir, XcronPeersFile)
	if err := XcronConf.PeerConf.initPeerFile(peerFile); err != nil {
		log.Printf("[error] peer initPeerFile err:%s", err.Error())
		return err
	}
	return nil
}

type config struct {
	Env         string      `toml:"env"`
	HttpPort    string      `toml:"http_port"`
	MonitorPort string      `toml:"monitor_port"`
	PeerPort    string      `toml:"peer_port"`
	RaftDir     string      `toml:"raft_dir"`
	PeerConf    peerConfig  `toml:"peer"`
	GroupConf   groupConfig `toml:"group"`
}

func (conf config) AddPeer(peer string) error {
	return conf.PeerConf.addPeer(peer)
}

func (conf config) GetPeers() []string {
	return conf.PeerConf.getPeers()
}

func (conf config) String() string {
	ret, _ := json.Marshal(conf)
	return string(ret)
}

type peerConfig struct {
	mutex     sync.Mutex
	PeerHosts []string `toml:"peer_hosts"` //raft host
}

func (peer peerConfig) getPeers() []string {
	peer.mutex.Lock()
	defer peer.mutex.Unlock()
	if peer.PeerHosts == nil {
		peer.PeerHosts = make([]string, 0)
	}
	return peer.PeerHosts
}

func (peer peerConfig) addPeer(p string) error {
	peer.mutex.Lock()
	defer peer.mutex.Unlock()
	if peer.PeerHosts == nil {
		peer.PeerHosts = make([]string, 0)
	}
	peer.PeerHosts = append(peer.PeerHosts, p)
	return nil
}

func (peer peerConfig) initPeerFile(file string) error {
	if peer.PeerHosts == nil {
		peer.PeerHosts = make([]string, 0)
	}
	peers, err := ioutil.ReadFile(filepath.Join(XcronConf.RaftDir, XcronPeersFile))
	if err == nil {
		log.Printf("[info] read config peers hosts:%s from file peers.json", string(peers))
		if err = json.Unmarshal(peers, &peer.PeerHosts); err != nil {
			log.Printf("[error] json.unmarshal peers err:%s", err.Error())
			return err
		}
		return nil
	}
	addr := fmt.Sprintf("%s:%s", XcronState.GetLocalAddr(), XcronConf.PeerConf)
	var exist bool
	for _, i := range peer.PeerHosts {
		if i == addr {
			exist = true
			break
		}
	}
	if !exist {
		peer.PeerHosts = append(peer.PeerHosts, addr)
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(&peer.PeerHosts); err != nil {
		return err
	}
	return ioutil.WriteFile(file, buf.Bytes(), 0755)
}

type groupConfig struct {
	GroupId          string   `toml:"group_id"`   //group relation
	GroupRole        string   `toml:"group_role"` // master and slave role
	GroupMasterHosts []string `toml:"group_master_hosts"`
}
