package ifs

import (
	"fmt"
	"strings"
	"strconv"
	"github.com/gorilla/websocket"
	"github.com/orcaman/concurrent-map"
)

type AgentConnectionPool struct {
	Connections      cmap.ConcurrentMap
	ReceivedChannels cmap.ConcurrentMap
	SendingChannels  cmap.ConcurrentMap
}

func NewAgentConnectionPool() *AgentConnectionPool {
	return &AgentConnectionPool{
		Connections:      cmap.New(),
		ReceivedChannels: cmap.New(),
		SendingChannels:  cmap.New(),
	}
}


func (p *AgentConnectionPool) Set(index uint8, conn *websocket.Conn) {
	p.Connections.Set(strconv.FormatUint(uint64(index), 10), conn)
	p.ReceivedChannels.Set(strconv.FormatUint(uint64(index), 10), make(chan *Packet, ChannelLength))
	p.SendingChannels.Set(strconv.FormatUint(uint64(index), 10), make(chan *Packet, ChannelLength))
}

func (p *AgentConnectionPool) Remove(index uint8) {
	p.Connections.Remove(strconv.FormatUint(uint64(index), 10))
	p.SendingChannels.Remove(strconv.FormatUint(uint64(index), 10))
	p.ReceivedChannels.Remove(strconv.FormatUint(uint64(index), 10))
}

type FsConnectionPool struct {
	Connections      []*websocket.Conn
	ReceivedChannels []chan *PacketChannelTuple
	SendingChannels  []chan *PacketChannelTuple
}

func newFsConnectionPool() *FsConnectionPool {
	return &FsConnectionPool{}
}

func (p *FsConnectionPool) Append(conn *websocket.Conn) {
	p.Connections = append(p.Connections, conn)
	p.ReceivedChannels = append(p.ReceivedChannels, make(chan *PacketChannelTuple, ChannelLength))
	p.SendingChannels = append(p.SendingChannels, make(chan *PacketChannelTuple, ChannelLength))
}

func (p *FsConnectionPool) Len() int {
	return len(p.Connections)
}

type PacketChannelTuple struct {
	Packet  *Packet
	Channel chan *Packet
}

type RemotePath struct {
	Hostname string
	Port     uint16
	Path     string
}

func (rp *RemotePath) String() string {
	return fmt.Sprintf("%s:%d@%s", rp.Hostname, rp.Port, rp.Path)
}

func (rp *RemotePath) Convert(str string) {
	parts := strings.Split(str, ":")
	rp.Hostname = parts[0]
	parts = strings.Split(parts[1], "@")
	p64, _ := strconv.ParseUint(parts[0], 10, 32)
	rp.Port = uint16(p64)
	rp.Path = parts[1]
}

func (rp *RemotePath) Address() string {
	return fmt.Sprintf("%s:%d", rp.Hostname, rp.Port)
}

type LoadRequest struct {
	key string
	ch  chan *LoadResponse
}

type LoadResponse struct {
	val interface{}
	ok  bool
}

type SetRequest struct {
	key string
	val interface{}
}

type FastMap struct {
	Map           map[string]interface{}
	ReadChannel   chan *LoadRequest
	WriteChannel  chan *SetRequest
	DeleteChannel chan string
}

func NewFastMap() *FastMap {
	fastMap := &FastMap{
		Map:           make(map[string]interface{}),
		ReadChannel:   make(chan *LoadRequest, ChannelLength),
		WriteChannel:  make(chan *SetRequest, ChannelLength),
		DeleteChannel: make(chan string, ChannelLength),
	}
	go fastMap.ProcessRequests()
	return fastMap
}

func (f *FastMap) ProcessRequests() {
	for {

		select {
		case key := <-f.DeleteChannel:
			delete(f.Map, key)
		case req := <-f.WriteChannel:
			f.Map[req.key] = req.val
		case req := <-f.ReadChannel:
			val, ok := f.Map[req.key]

			req.ch <- &LoadResponse{
				val: val,
				ok:  ok,
			}
		}
	}
}

func (f *FastMap) Set(key string, val interface{}) {
	f.WriteChannel <- &SetRequest{
		key: key,
		val: val,
	}
}

func (f *FastMap) Load(key string) (interface{}, bool) {
	ch := make(chan *LoadResponse)
	f.ReadChannel <- &LoadRequest{
		key: key,
		ch:  ch,
	}
	resp := <-ch
	return resp.val, resp.ok
}

func (f *FastMap) Delete(key string) {
	f.DeleteChannel <- key
}
