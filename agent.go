package ifs

import (
	"net/http"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"strconv"
)

type Agent struct {
	Pool *AgentConnectionPool
}

func populateResponse(resp *Packet, data Payload, err error) {

	if err == nil {
		resp.Data = data
	} else {
		resp.Data = &Error{
			Err: err,
		}
	}
}

func (a *Agent) Listen(index int) {

	conn := a.Pool.Connections[index]

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

			go a.ProcessRequest(req, index)
		}

	}

}

func (a *Agent) HandleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, _ := upgrader.Upgrade(w, r, nil)

	log.WithFields(log.Fields{
		"address": conn.RemoteAddr().String(),
	}).Debug("Got New Connection")

	// TODO Sync Mutex
	a.Pool.Append(conn)

	i := len(a.Pool.Connections) - 1
	go a.Listen(i)
	go a.ProcessResponses(i)
}

func (a *Agent) ProcessRequest(req *Packet, index int) {

	resp := &Packet{
		ConnId: req.ConnId,
		Id: req.Id,
	}

	var data Payload
	var err error

	switch req.Op {

	case AttrRequest:
		resp.Op = StatResponse
		data, err = Attr(req)

	case ReadDirRequest:
		resp.Op = StatsResponse
		data, err = ReadDir(req)

	case FetchFileRequest:
		resp.Op = FileDataResponse
		data, err = FetchFile(req)

	case ReadFileRequest:
		resp.Op = FileDataResponse
		data, err = ReadFile(req)

	case WriteFileRequest:
		resp.Op = WriteResponse
		data, err = WriteFile(req)

	case SetAttrRequest:
		resp.Op = ErrorResponse
		err = SetAttr(req)

	case CreateRequest:
		resp.Op = ErrorResponse
		err = CreateFile(req)

	case RemoveRequest:
		resp.Op = ErrorResponse
		err = RemoveFile(req)
	}

	populateResponse(resp, data, err)

	a.Pool.SendingChannels[GetRandomIndex(len(a.Pool.Connections))] <- resp

}

func (a *Agent) ProcessResponses(index int) {
	log.WithFields(log.Fields{
		"index": index,
	}).Info("Starting Response Processor")

	respChan := a.Pool.SendingChannels[index]
	for resp := range respChan {
		data, _ := resp.Marshal()
		err := a.Pool.Connections[index].WriteMessage(websocket.BinaryMessage, data)
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

func StartAgent(address string, port int) {
	agent := &Agent{
		Pool: newAgentConnectionPool(),
	}

	log.WithFields(log.Fields{
		"address": address,
		"port":    port,
	}).Info("Starting Agent")


	http.HandleFunc("/", agent.HandleRequests)
	err := http.ListenAndServe(address+":"+strconv.FormatInt(int64(port), 10), nil)

	if err != nil {
		log.Fatal(err)
	}

}
