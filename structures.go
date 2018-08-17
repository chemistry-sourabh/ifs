/*
Copyright 2018 Sourabh Bollapragada

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ifs

import (
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/orcaman/concurrent-map"
	"strconv"
	"strings"
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
