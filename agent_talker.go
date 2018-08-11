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
	"net/http"
	"strconv"
	"github.com/gorilla/websocket"
	"sync/atomic"
	"go.uber.org/zap"
	"strings"
	"sync"
	)

type agentTalker struct {
	IdCounter uint64
	Pool      *AgentConnectionPool
}

var (
	agentTalkerInstance *agentTalker
	agentTalkerOnce     sync.Once
)

func AgentTalker() *agentTalker {
	agentTalkerOnce.Do(func() {
		agentTalkerInstance = &agentTalker{
			Pool: NewAgentConnectionPool(),
		}
	})

	return agentTalkerInstance
}

func (t *agentTalker) Startup(address string, port uint16) {

	http.HandleFunc("/", t.HandleRequests)
	err := http.ListenAndServe(address+":"+strconv.FormatInt(int64(port), 10), nil)

	if err != nil {
		zap.L().Fatal("Listen and Serve Failed",
			zap.Error(err),
		)
	}

}

func (t *agentTalker) processSendingChannel(index uint8) {

	zap.L().Debug("Starting Egress Processor",
		zap.Uint8("index", index),
	)

	val, _ := t.Pool.SendingChannels.Get(strconv.FormatUint(uint64(index), 10))
	pktChan := val.(chan *Packet)
	for pkt := range pktChan {

		if pkt.IsRequest() {
			pkt.Id = atomic.AddUint64(&t.IdCounter, 1)
			pkt.ConnId = index
		}

		zap.L().Debug("Sending Packet",
			zap.Uint8("index", index),
			zap.String("op", strings.ToLower(ConvertOpCodeToString(pkt.Op))),
			zap.Uint8("conn_id", pkt.ConnId),
			zap.Bool("request", pkt.IsRequest()),
			zap.Uint64("id", pkt.Id),
		)

		data, _ := pkt.Marshal()
		val, _ := t.Pool.Connections.Get(strconv.FormatUint(uint64(index), 10))
		conn := val.(*websocket.Conn)
		err := conn.WriteMessage(websocket.BinaryMessage, data)

		if err != nil {
			zap.L().Fatal("Write Message Failed",
				zap.Error(err),
			)
		}

	}
}

func (t *agentTalker) Listen(index uint8) {

	val, _ := t.Pool.Connections.Get(strconv.FormatUint(uint64(index), 10))
	conn := val.(*websocket.Conn)

	for {

		req := &Packet{}

		zap.L().Debug("Listening for Packet",
			zap.String("address", conn.RemoteAddr().String()),
		)

		typ, data, err := conn.ReadMessage()

		if err != nil {
			zap.L().Warn("Read Message Failed",
				zap.Error(err),
			)
			break
		}

		if typ == websocket.BinaryMessage {
			req.Unmarshal(data)

			zap.L().Debug("Received Packet",
				zap.Uint8("index", index),
				zap.String("op", strings.ToLower(ConvertOpCodeToString(req.Op))),
				zap.Uint8("conn_id", req.ConnId),
				zap.Bool("request", req.IsRequest()),
				zap.Uint64("id", req.Id),
			)

			go Agent().ProcessRequest(req)
		}

	}

	t.Pool.Remove(index)
	AgentFileHandler().CloseAll()

}

func (t *agentTalker) HandleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	upgrader.EnableCompression = true
	conn, _ := upgrader.Upgrade(w, r, nil)

	zap.L().Debug("Got New Connection",
		zap.String("address", conn.RemoteAddr().String()),
	)

	i := uint8(t.Pool.Connections.Count())

	conn.SetPingHandler(func(appData string) error {

		zap.L().Debug("Got Ping",
			zap.String("msg", appData),
			zap.Uint8("index", i),
		)

		return nil
	})

	t.Pool.Set(i, conn)

	go t.Listen(i)
	go t.processSendingChannel(i)
}

func (t *agentTalker) SendPacket(pkt *Packet) {
	val, _ := t.Pool.SendingChannels.Get(strconv.FormatUint(uint64(GetRandomIndex(t.Pool.Connections.Count())), 10))
	val.(chan *Packet) <- pkt
}
