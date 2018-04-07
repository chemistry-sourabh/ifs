package arsyncfs

import (
	"net/url"
	"github.com/gorilla/websocket"
	"log"
	"github.com/mitchellh/mapstructure"
)

//type connectionPool struct {
//	connections []*websocket.Conn
//}
//
//func (cp *connectionPool) pickRandomConnection() *websocket.Conn {
//	rand.Seed(time.Now().UnixNano())
//	return cp.connections[rand.Intn(len(cp.connections))]
//}
//
//func (cp *connectionPool) listen() *Response {
//
//}

type Talker struct {
	// Should be map of hostname and port
	Ifs                  *Ifs
	idCounter            uint64
	connection           *websocket.Conn
	requestBuffer        chan *Request // One Receiver for each pool ?
	egressRequestChannel chan *Request
}

func (t *Talker) Startup(address string) {

	t.egressRequestChannel = make(chan *Request, ChannelLength)
	t.requestBuffer = make(chan *Request, ChannelLength)
	t.mountRemoteRoot(address)
	t.idCounter = 0

	go t.processEgressChannel()
	go t.processIncomingMessages()

}

func (t *Talker) mountRemoteRoot(address string) {

	u := url.URL{Scheme: "ws", Host: address, Path: "/"}

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

	if err != nil {
		log.Fatal(err)
	}

	t.connection = c

}

func (t *Talker) sendRequest(opCode uint8, rn *RemoteNode) BaseResponse {

	respChannel := make(chan BaseResponse)

	req := &Request{
		Op:              opCode,
		RemoteNode:      rn,
		ResponseChannel: respChannel,
	}

	t.egressRequestChannel <- req

	return <-respChannel
}

func (t *Talker) processEgressChannel() {

	log.Println("Starting Egress Channel Processor")

	for req := range t.egressRequestChannel {
		log.Printf("Sending Request With Op Code %d and Remote Path %s", req.Op, req.RemoteNode.RemotePath.String())

		req.Id = t.idCounter
		t.idCounter++

		err := t.connection.WriteJSON(req)
		if err != nil {
			log.Fatal(err)
		}

		t.requestBuffer <- req

	}
}

func (t *Talker) processIncomingMessages() {
	log.Printf("Starting Incoming Message Processor")

	localRequests := make(map[uint64]*Request)

	for {

		mp := make(map[string]interface{})

		err := t.connection.ReadJSON(&mp)

		if err != nil {
			log.Fatal(err)
			break
		}

		requestId := uint64(mp["request_id"].(float64))

		log.Printf("Received Response for RequestId %d", requestId)
		req, ok := localRequests[requestId]

		if !ok {
			for req = range t.requestBuffer {
				if req.Id == requestId {
					break
				} else {
					localRequests[req.Id] = req
				}
			}
		}

		resp := t.convertResponse(req.Op, mp)
		req.ResponseChannel <- resp
		close(req.ResponseChannel)

	}

}

func (t *Talker) convertResponse(opCode uint8, mp map[string]interface{}) BaseResponse {

	var resp BaseResponse
	var err error

	switch opCode {
	case AttrOp:
		resp = &StatResponse{}
		err = mapstructure.Decode(mp, resp)

	case ReadDirOp:
		resp = &ReadDirResponse{}
		err = mapstructure.Decode(mp, resp)

	case FetchFileOp:
		resp = &FileDataResponse{}
		err = mapstructure.Decode(mp, resp)

	}

	if err != nil {
		log.Fatal(err)
	}

	return resp
}
