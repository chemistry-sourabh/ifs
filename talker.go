package ifs

import (
	"net/url"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"strings"
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
	requestBuffer        chan *PacketChannelTuple // One Receiver for each pool ?
	egressRequestChannel chan *PacketChannelTuple
	stillConnected       bool
}

func (t *Talker) Startup(address string) {

	t.egressRequestChannel = make(chan *PacketChannelTuple, ChannelLength)
	t.requestBuffer = make(chan *PacketChannelTuple, ChannelLength)
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

func (t *Talker) sendRequest(opCode uint8, payload Payload) *Packet {

	respChannel := make(chan *Packet)

	req := &Packet{
		Op:   opCode,
		Data: payload,
	}

	t.egressRequestChannel <- &PacketChannelTuple{
		req,
		respChannel,
	}

	return <-respChannel
}

func (t *Talker) processEgressChannel() {

	log.Info("Starting Egress Channel Processor")

	for req := range t.egressRequestChannel {

		pkt, _ := req.Packet, req.Channel

		pkt.Id = t.idCounter
		t.idCounter++

		log.WithFields(log.Fields{
			"op": strings.ToLower(ConvertOpCodeToString(pkt.Op)),
			"id": pkt.Id,
		}).Debug("Sending Packet")

		data, _ := pkt.Marshal()
		err := t.connection.WriteMessage(websocket.BinaryMessage, data)
		if err != nil {
			log.Fatal(err)
		}

		t.requestBuffer <- req

	}
}

func (t *Talker) processIncomingMessages() {
	log.Info("Starting Incoming Message Processor")

	localRequests := make(map[uint64]chan *Packet)

	for {

		resp := &Packet{}

		_, data, err := t.connection.ReadMessage()

		if err != nil {
			log.Fatal(err)
			break
		}

		resp.Unmarshal(data)

		log.WithFields(log.Fields{
			"op": strings.ToLower(ConvertOpCodeToString(resp.Op)),
			"id": resp.Id,
		}).Debug("Received Request")

		ch, ok := localRequests[resp.Id]

		if !ok {
			for req := range t.requestBuffer {
				pkt, channel := req.Packet, req.Channel

				if pkt.Id == resp.Id {
					ch = channel
					break
				} else {
					localRequests[pkt.Id] = ch
				}
			}
		} else {
			delete(localRequests, resp.Id)
		}

		ch <- resp
		close(ch)

	}

}
