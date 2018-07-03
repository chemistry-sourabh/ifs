package ifs

import (
	"net/http"
	"strconv"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
)

type AgentTalker struct {
	IdCounter uint64
	Agent *Agent
	Pool *AgentConnectionPool
}

func (t *AgentTalker) Startup(address string, port uint16) {

	http.HandleFunc("/", t.HandleRequests)
	err := http.ListenAndServe(address+":"+strconv.FormatInt(int64(port), 10), nil)

	if err != nil {
		log.Fatal(err)
	}

}

func (t *AgentTalker) processSendingChannel(index uint8) {
	log.WithFields(log.Fields{
		"index": index,
	}).Info("Starting Response Processor")

	pktChan := t.Pool.SendingChannels[index]
	for pkt := range pktChan {

		if pkt.IsRequest() {
			pkt.Id = atomic.AddUint64(&t.IdCounter, 1)
			pkt.ConnId = index
		}

		data, _ := pkt.Marshal()
		err := t.Pool.Connections[index].WriteMessage(websocket.BinaryMessage, data)
		log.WithFields(log.Fields{
			"conn_id": pkt.ConnId,
			"request": pkt.IsRequest(),
			"id":      pkt.Id,
			"op":      ConvertOpCodeToString(pkt.Op),
			"index":   index,
		}).Debug("Sent Packet")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (t *AgentTalker) Listen(index uint8) {

	conn := t.Pool.Connections[index]

	log.WithFields(log.Fields{
		"address": conn.RemoteAddr(),
	}).Info("Listening for Packets")

	for {

		req := &Packet{}

		typ, data, err := conn.ReadMessage()

		if err != nil {
			log.Fatal(err)
			break
		}

		if typ == websocket.BinaryMessage {
			req.Unmarshal(data)
			log.WithFields(log.Fields{
				"conn_id": req.ConnId,
				"id": req.Id,
				"op": ConvertOpCodeToString(req.Op),
				"index": index,
			}).Debug("Received Packet")

			go t.Agent.ProcessRequest(req)
		}

	}

}

func (t *AgentTalker) HandleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, _ := upgrader.Upgrade(w, r, nil)

	log.WithFields(log.Fields{
		"address": conn.RemoteAddr().String(),
	}).Debug("Got New Connection")

	t.Pool.Append(conn)

	i := uint8(len(t.Pool.Connections) - 1)
	go t.Listen(i)
	go t.processSendingChannel(i)
}

func (t *AgentTalker) SendPacket(pkt *Packet) {
	t.Pool.SendingChannels[GetRandomIndex(len(t.Pool.Connections))] <- pkt
}

