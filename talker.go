package arsyncfs

import (
	"net/url"
	"github.com/gorilla/websocket"
	"log"
)

type Talker struct {
	// Should be map of hostname and port
	idCounter              uint64
	WebSockets             map[string]*websocket.Conn
	IngressRequestChannels map[string]chan *Request
	EgressRequestChannel   chan *Request
}

func (t *Talker) MountRemoteRoots(paths []*RemotePath) {

	for _, path := range paths {

		u := url.URL{Scheme: "ws", Host: path.Address(), Path: "/"}
		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

		if err != nil {
			log.Fatal(err)
		}

		t.WebSockets[path.Address()] = c
	}

}

func (t *Talker) ProcessChannel() {

	log.Println("Starting Egress Channel Processor")

	for req := range t.EgressRequestChannel {
		log.Printf("Processing Request With Op Code %d and Remote Path %s", req.Op, req.RemoteNode.RemotePath.String())
		t.sendRequest(req)
	}
}

func (t *Talker) sendRequest(req *Request) {

	req.Id = t.idCounter
	t.idCounter++

	err := t.WebSockets[req.RemoteNode.RemotePath.Address()].WriteJSON(req)
	if err != nil {
		log.Fatal(err.Error())
	}

	t.IngressRequestChannels[req.RemoteNode.RemotePath.Address()] <- req
}

func (t *Talker) ProcessAgentMessages(address string) {
	log.Printf("Starting Ingress Channel Processor for %s", address)

	localRequests := make(map[uint64]*Request)

	for {

		resp := &Response{}

		err := t.WebSockets[address].ReadJSON(resp)

		if err != nil {
			log.Fatal(err)
			break
		}

		req, ok := localRequests[resp.Id]

		if ok {
			req.ResponseChannel <- resp
		} else {
			for req = range t.IngressRequestChannels[address] {
				if req.Id == resp.Id {
					req.ResponseChannel <- resp
					break
				} else {
					localRequests[req.Id] = req
				}
			}
		}

		close(req.ResponseChannel)

	}

}
