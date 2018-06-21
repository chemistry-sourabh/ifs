package ifs

import (
	"net/http"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"strconv"
)

type Agent struct {
	Pool *AgentConnectionPool
	FileHandler *AgentFileHandler
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
		data, err = a.FileHandler.Attr(req)

	case ReadDirRequest:
		resp.Op = StatsResponse
		data, err = a.FileHandler.ReadDir(req)

	case FetchFileRequest:
		resp.Op = FileDataResponse
		data, err = a.FileHandler.FetchFile(req)

	case ReadFileRequest:
		resp.Op = FileDataResponse
		data, err = a.FileHandler.ReadFile(req)

	case WriteFileRequest:
		resp.Op = WriteResponse
		data, err = a.FileHandler.WriteFile(req)

	case SetAttrRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.SetAttr(req)

	case CreateRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.CreateFile(req)

	case RemoveRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.RemoveFile(req)

	case RenameRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.RenameFile(req)

	case OpenRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.OpenFile(req)
	case CloseRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.CloseFile(req)

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

func StartAgent(address string, port uint16) {
	agent := &Agent{
		Pool: newAgentConnectionPool(),
		FileHandler: NewAgentFileHandler(),
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
