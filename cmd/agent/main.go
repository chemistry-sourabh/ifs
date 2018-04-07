package main

import (
	"github.com/gorilla/websocket"
	"net/http"
	"arsyncfs"
	"log"
)

type Agent struct {
	Connection *websocket.Conn
	RequestChannel chan *arsyncfs.Request
	ResponseChannel chan arsyncfs.BaseResponse
}

func (a *Agent) HandleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, _ := upgrader.Upgrade(w, r, nil)

	log.Printf("Got New Connection at %s", conn.LocalAddr().String())

	a.Connection = conn

	for {

		req := &arsyncfs.Request{}

		err := conn.ReadJSON(req)

		if err != nil {
			log.Fatal(err.Error())
			break
		}

		a.RequestChannel <- req

	}

	conn.Close()
}

func (a *Agent) ProcessRequests() {
	log.Println("Starting Request Processor")
	for req := range a.RequestChannel {

		var resp arsyncfs.BaseResponse

		switch req.Op {

		case arsyncfs.AttrOp:
			resp = arsyncfs.Attr(req)

		case arsyncfs.ReadDirOp:
			resp = arsyncfs.ReadDir(req)

		case arsyncfs.FetchFileOp:
			resp = arsyncfs.FetchFile(req)

		case arsyncfs.ReadFileOp:


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
		err := a.Connection.WriteJSON(resp)
		log.Printf("Sent Response for Op Code %d With RequestId %d", resp.Op(), resp.Id())
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {

	agent := &Agent{
		RequestChannel: make(chan *arsyncfs.Request, arsyncfs.ChannelLength),
		ResponseChannel: make(chan arsyncfs.BaseResponse, arsyncfs.ChannelLength),
	}

	log.Println("Starting Server")

	go agent.ProcessRequests()
	go agent.ProcessResponses()

	http.HandleFunc("/", agent.HandleRequests)
	http.ListenAndServe("0.0.0.0:8000", nil)

}
