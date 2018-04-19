package ifs

import (
	"net/http"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"strconv"
)

type Agent struct {
	Connection      *websocket.Conn
	RequestChannel  chan *Packet
	ResponseChannel chan *Packet
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

func (a *Agent) HandleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, _ := upgrader.Upgrade(w, r, nil)

	log.WithFields(log.Fields{
		"address": conn.RemoteAddr().String(),
	}).Debug("Got New Connection")

	a.Connection = conn

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
				"id": req.Id,
				"op": ConvertOpCodeToString(req.Op),
			}).Debug("Received Packet")

			go a.ProcessRequest(req)
		}

	}

	conn.Close()
}

func (a *Agent) ProcessRequest(req *Packet) {

	resp := &Packet{
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

	a.ResponseChannel <- resp

}

func (a *Agent) ProcessResponses(respChan chan *Packet) {
	log.Println("Starting Response Processor")
	for resp := range respChan {
		data, _ := resp.Marshal()
		err := a.Connection.WriteMessage(websocket.BinaryMessage, data)
		log.WithFields(log.Fields{
			"id": resp.Id,
			"op": ConvertOpCodeToString(resp.Op),
		}).Debug("Sent Packet")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func StartAgent(address string, port int) {
	agent := &Agent{
		RequestChannel:  make(chan *Packet, ChannelLength),
		ResponseChannel: make(chan *Packet, ChannelLength),
	}

	log.WithFields(log.Fields{
		"address": address,
		"port":    port,
	}).Info("Starting Agent")

	go agent.ProcessResponses(agent.ResponseChannel)

	http.HandleFunc("/", agent.HandleRequests)
	err := http.ListenAndServe(address+":"+strconv.FormatInt(int64(port), 10), nil)

	if err != nil {
		log.Fatal(err)
	}

}
