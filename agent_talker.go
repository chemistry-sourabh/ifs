package ifs

import (
	"net/http"
	"strconv"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

type AgentTalker struct {
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

func (t *AgentTalker) processSendingChannel(index int) {
	log.WithFields(log.Fields{
		"index": index,
	}).Info("Starting Response Processor")

	respChan := t.Pool.SendingChannels[index]
	for resp := range respChan {
		data, _ := resp.Marshal()
		err := t.Pool.Connections[index].WriteMessage(websocket.BinaryMessage, data)
		log.WithFields(log.Fields{
			"conn_id": resp.ConnId,
			"id": resp.Id,
			"op": ConvertOpCodeToString(resp.Op),
			"index": index,
		}).Debug("Sent Packet")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (t *AgentTalker) Listen(index int) {

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

	i := len(t.Pool.Connections) - 1
	go t.Listen(i)
	go t.processSendingChannel(i)
}

func (t *AgentTalker) SendResponse(resp *Packet) {
	t.Pool.SendingChannels[GetRandomIndex(len(t.Pool.Connections))] <- resp
}

