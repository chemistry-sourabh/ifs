package ifs

import (
	"fmt"
	"strings"
	"strconv"
	"github.com/gorilla/websocket"
)

type AgentConnectionPool struct {
	Connections      []*websocket.Conn
	ReceivedChannels []chan *Packet
	SendingChannels  []chan *Packet
}

func (p *AgentConnectionPool) Append(conn *websocket.Conn) {
	p.Connections = append(p.Connections, conn)
	p.ReceivedChannels = append(p.ReceivedChannels, make(chan *Packet, ChannelLength))
	p.SendingChannels = append(p.SendingChannels, make(chan *Packet, ChannelLength))
}

type FsConnectionPool struct {
	Connections      []*websocket.Conn
	ReceivedChannels []chan *PacketChannelTuple
	SendingChannels  []chan *PacketChannelTuple
}

func (p *FsConnectionPool) Append(conn *websocket.Conn) {
	p.Connections = append(p.Connections, conn)
	p.ReceivedChannels = append(p.ReceivedChannels, make(chan *PacketChannelTuple, ChannelLength))
	p.SendingChannels = append(p.SendingChannels, make(chan *PacketChannelTuple, ChannelLength))
}

type PacketChannelTuple struct {
	Packet  *Packet
	Channel chan *Packet
}

type RemotePath struct {
	Hostname string
	Port     uint32
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
	rp.Port = uint32(p64)
	rp.Path = parts[1]
}

func (rp *RemotePath) Address() string {
	return fmt.Sprintf("%s:%d", rp.Hostname, rp.Port)
}
