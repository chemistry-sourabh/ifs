package main

import (
	"github.com/gorilla/websocket"
	"net/http"
	"arsyncfs"
	"log"
)

type Agent struct {
	Connection      *websocket.Conn
	RequestChannel  chan *arsyncfs.Packet
	ResponseChannel chan *arsyncfs.Packet
}

func populateResponse(resp *arsyncfs.Packet, data arsyncfs.Payload, err error) {

	if err == nil {
		resp.Data = data
	} else {
		resp.Data = err
	}

}

func (a *Agent) HandleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, _ := upgrader.Upgrade(w, r, nil)

	log.Printf("Got New Connection at %s", conn.LocalAddr().String())

	a.Connection = conn

	for {

		req := &arsyncfs.Packet{}

		typ, data, err := conn.ReadMessage()

		if err != nil {
			log.Fatal(err)
			break
		}

		if typ == websocket.BinaryMessage {
			req.Unmarshal(data)
			log.Printf("Received Packet with Id %d and Op %s", req.Id, arsyncfs.ConvertOpCodeToString(req.Op))
			a.RequestChannel <- req
		}

	}

	conn.Close()
}

func (a *Agent) ProcessRequests() {
	log.Println("Starting Request Processor")
	for req := range a.RequestChannel {

		resp := &arsyncfs.Packet{
			Id: req.Id,
		}

		var data arsyncfs.Payload
		var err error

		switch req.Op {

		case arsyncfs.AttrRequest:
			resp.Op = arsyncfs.StatResponse
			data, err = arsyncfs.Attr(req)


		case arsyncfs.ReadDirRequest:
			resp.Op = arsyncfs.StatsResponse
			data, err = arsyncfs.ReadDir(req)

		case arsyncfs.FetchFileRequest:
			resp.Op = arsyncfs.FileDataResponse
			data, err = arsyncfs.FetchFile(req)

		case arsyncfs.ReadFileRequest:
			resp.Op = arsyncfs.FileDataResponse
			data, err = arsyncfs.ReadFile(req)

		case arsyncfs.WriteFileRequest:
			resp.Op = arsyncfs.WriteResponse
			data, err = arsyncfs.WriteFile(req)

		case arsyncfs.TruncateRequest:
			resp.Op = arsyncfs.ErrorResponse
			err = arsyncfs.Truncate(req)

		case arsyncfs.CreateRequest:
			resp.Op = arsyncfs.ErrorResponse
			err = arsyncfs.CreateFile(req)

		case arsyncfs.RemoveRequest:
			resp.Op = arsyncfs.ErrorResponse
			err = arsyncfs.RemoveFile(req)
		}

		populateResponse(resp, data, err)

		a.ResponseChannel <- resp
	}

}

func (a *Agent) ProcessResponses() {
	log.Println("Starting Response Processor")
	for resp := range a.ResponseChannel {
		err := a.Connection.WriteMessage(websocket.BinaryMessage, resp.Marshal())
		log.Printf("Sent Packet Id %d with Op %s", resp.Id, arsyncfs.ConvertOpCodeToString(resp.Op))
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	//log.SetOutput(ioutil.Discard)
	agent := &Agent{
		RequestChannel:  make(chan *arsyncfs.Packet, arsyncfs.ChannelLength),
		ResponseChannel: make(chan *arsyncfs.Packet, arsyncfs.ChannelLength),
	}

	log.Println("Starting Server")

	go agent.ProcessRequests()
	go agent.ProcessResponses()

	http.HandleFunc("/", agent.HandleRequests)
	http.ListenAndServe("0.0.0.0:8000", nil)

}
