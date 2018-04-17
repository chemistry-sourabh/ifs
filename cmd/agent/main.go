package main

import (
	"github.com/gorilla/websocket"
	"net/http"
	"ifs"
	"log"
)

type Agent struct {
	Connection      *websocket.Conn
	RequestChannel  chan *ifs.Packet
	ResponseChannel chan *ifs.Packet
}

func populateResponse(resp *ifs.Packet, data ifs.Payload, err error) {

	if err == nil {
		resp.Data = data
	} else {
		resp.Data = &ifs.Error{
			Err: err,
		}
	}
}

func (a *Agent) HandleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, _ := upgrader.Upgrade(w, r, nil)

	log.Printf("Got New Connection at %s", conn.LocalAddr().String())

	a.Connection = conn

	for {

		req := &ifs.Packet{}

		typ, data, err := conn.ReadMessage()

		if err != nil {
			log.Fatal(err)
			break
		}

		if typ == websocket.BinaryMessage {
			req.Unmarshal(data)
			log.Printf("Received Packet with Id %d and Op %s", req.Id, ifs.ConvertOpCodeToString(req.Op))
			a.RequestChannel <- req
		}

	}

	conn.Close()
}

func (a *Agent) ProcessRequests() {
	log.Println("Starting Request Processor")
	for req := range a.RequestChannel {

		resp := &ifs.Packet{
			Id: req.Id,
		}

		var data ifs.Payload
		var err error

		switch req.Op {

		case ifs.AttrRequest:
			resp.Op = ifs.StatResponse
			data, err = ifs.Attr(req)

		case ifs.ReadDirRequest:
			resp.Op = ifs.StatsResponse
			data, err = ifs.ReadDir(req)

		case ifs.FetchFileRequest:
			resp.Op = ifs.FileDataResponse
			data, err = ifs.FetchFile(req)

		case ifs.ReadFileRequest:
			resp.Op = ifs.FileDataResponse
			data, err = ifs.ReadFile(req)

		case ifs.WriteFileRequest:
			resp.Op = ifs.WriteResponse
			data, err = ifs.WriteFile(req)

		case ifs.SetAttrRequest:
			resp.Op = ifs.ErrorResponse
			err = ifs.SetAttr(req)

		case ifs.CreateRequest:
			resp.Op = ifs.ErrorResponse
			err = ifs.CreateFile(req)

		case ifs.RemoveRequest:
			resp.Op = ifs.ErrorResponse
			err = ifs.RemoveFile(req)
		}

		populateResponse(resp, data, err)

		a.ResponseChannel <- resp
	}

}

func (a *Agent) ProcessResponses() {
	log.Println("Starting Response Processor")
	for resp := range a.ResponseChannel {
		data, _ := resp.Marshal()
		err := a.Connection.WriteMessage(websocket.BinaryMessage, data)
		log.Printf("Sent Packet Id %d with Op %s", resp.Id, ifs.ConvertOpCodeToString(resp.Op))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	//log.SetOutput(ioutil.Discard)
	agent := &Agent{
		RequestChannel:  make(chan *ifs.Packet, ifs.ChannelLength),
		ResponseChannel: make(chan *ifs.Packet, ifs.ChannelLength),
	}

	log.Println("Starting Server")

	go agent.ProcessRequests()
	go agent.ProcessResponses()

	http.HandleFunc("/", agent.HandleRequests)
	err := http.ListenAndServe("0.0.0.0:8000", nil)

	if err != nil {
		log.Fatal(err)
	}

}
