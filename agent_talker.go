package ifs

import (
	"net/http"
	"strconv"
	"github.com/gorilla/websocket"
	"sync/atomic"
	"go.uber.org/zap"
	"strings"
)

type AgentTalker struct {
	IdCounter uint64
	Agent     *Agent
	Pool      *AgentConnectionPool
}

func (t *AgentTalker) Startup(address string, port uint16) {

	http.HandleFunc("/", t.HandleRequests)
	err := http.ListenAndServe(address+":"+strconv.FormatInt(int64(port), 10), nil)

	if err != nil {
		zap.L().Fatal("Listen and Serve Failed",
			zap.Error(err),
		)
	}

}

func (t *AgentTalker) processSendingChannel(index uint8) {

	zap.L().Debug("Starting Egress Processor",
		zap.Uint8("index", index),
	)

	pktChan := t.Pool.SendingChannels[index]
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
		err := t.Pool.Connections[index].WriteMessage(websocket.BinaryMessage, data)

		if err != nil {
			zap.L().Fatal("Write Message Failed",
				zap.Error(err),
			)
		}

	}
}

func (t *AgentTalker) Listen(index uint8) {

	conn := t.Pool.Connections[index]

	for {

		req := &Packet{}

		zap.L().Debug("Listening for Packet",
			zap.String("address", conn.RemoteAddr().String()),
		)

		typ, data, err := conn.ReadMessage()

		if err != nil {
			zap.L().Fatal("Read Message Failed",
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

			go t.Agent.ProcessRequest(req)
		}

	}

}

func (t *AgentTalker) HandleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, _ := upgrader.Upgrade(w, r, nil)

	zap.L().Debug("Got New Connection",
		zap.String("address", conn.RemoteAddr().String()),
	)

	t.Pool.Append(conn)

	i := uint8(len(t.Pool.Connections) - 1)
	go t.Listen(i)
	go t.processSendingChannel(i)
}

func (t *AgentTalker) SendPacket(pkt *Packet) {
	t.Pool.SendingChannels[GetRandomIndex(len(t.Pool.Connections))] <- pkt
}
