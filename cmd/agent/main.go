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

func (a *Agent) HandleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, _ := upgrader.Upgrade(w, r, nil)

	log.Printf("Got New Connection at %s", conn.LocalAddr().String())

	a.Connection = conn

	for {

		req := &arsyncfs.Packet{}

		typ, data, err := conn.ReadMessage()

		if err != nil {
			log.Fatal(err.Error())
			break
		}

		if typ == websocket.BinaryMessage {
			req.Unmarshal(data)
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

		switch req.Op {

		case arsyncfs.AttrRequest:
			resp.Op = arsyncfs.StatResponse
			resp.Data = arsyncfs.Attr(req)

		case arsyncfs.ReadDirRequest:
			resp.Op = arsyncfs.StatsResponse
			resp.Data = arsyncfs.ReadDir(req)

		case arsyncfs.FetchFileRequest:
			resp.Op = arsyncfs.FileDataResponse
			resp.Data = arsyncfs.FetchFile(req)

		case arsyncfs.ReadFileRequest:
			resp.Op = arsyncfs.FileDataResponse
			//resp =

		}

		log.Println("Going to Send Response on Channel")
		a.ResponseChannel <- resp
		log.Println("Response Sent on Channel")
	}

}

func (a *Agent) ProcessResponses() {
	log.Println("Starting Response Processor")
	for resp := range a.ResponseChannel {
		log.Println("Response Received On Channel")
		err := a.Connection.WriteMessage(websocket.BinaryMessage, resp.Marshal())
		log.Printf("Sent Response for Op Code %d With RequestId %d", resp.Op, resp.Id)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {

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
