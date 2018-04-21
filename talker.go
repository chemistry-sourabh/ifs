package ifs

import (
	"net/url"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"strings"
	"github.com/cornelk/hashmap"
	"unsafe"
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
	Ifs           *Ifs
	idCounter     uint64
	Pool          *FsConnectionPool
	requestBuffer *hashmap.HashMap
	//requestBuffer        chan *PacketChannelTuple // One Receiver for each pool ?
	//egressRequestChannel chan *PacketChannelTuple
}

func (t *Talker) Startup(address string, poolCount int) {

	//t.egressRequestChannel = make(chan *PacketChannelTuple, ChannelLength)
	//t.requestBuffer = make(chan *PacketChannelTuple, ChannelLength)
	t.requestBuffer = &hashmap.HashMap{}
	t.mountRemoteRoot(address, poolCount)
	t.idCounter = 0

}

func (t *Talker) mountRemoteRoot(address string, poolCount int) {

	u := url.URL{Scheme: "ws", Host: address, Path: "/"}

	for i := 0; i < poolCount; i++ {

		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

		if err != nil {
			log.Fatal(err)
		}

		t.Pool.Append(c)

		index := len(t.Pool.Connections) - 1
		go t.processEgressChannel(index)
		go t.processIncomingMessages(index)

	}
}


func (t *Talker) sendRequest(opCode uint8, payload Payload) *Packet {

	respChannel := make(chan *Packet)

	req := &Packet{
		Op:   opCode,
		Data: payload,
	}

	t.Pool.SendingChannels[GetRandomIndex(len(t.Pool.Connections))] <- &PacketChannelTuple{
		req,
		respChannel,
	}

	return <-respChannel
}

func (t *Talker) processEgressChannel(index int) {

	log.Info("Starting Egress Channel Processor")

	for req := range t.Pool.SendingChannels[index] {

		pkt, _ := req.Packet, req.Channel

		pkt.Id = t.idCounter
		t.idCounter++

		log.WithFields(log.Fields{
			"op": strings.ToLower(ConvertOpCodeToString(pkt.Op)),
			"id": pkt.Id,
		}).Debug("Sending Packet")

		t.requestBuffer.Set(pkt.Id, unsafe.Pointer(req))

		data, _ := pkt.Marshal()
		err := t.Pool.Connections[index].WriteMessage(websocket.BinaryMessage, data)
		if err != nil {
			log.Fatal(err)
		}


	}
}

func (t *Talker) processIncomingMessages(index int) {
	log.Info("Starting Incoming Message Processor")


	for {

		resp := &Packet{}

		log.Debug("Listening for Packet")

		_, data, err := t.Pool.Connections[index].ReadMessage()

		if err != nil {
			log.Fatal(err)
			break
		}

		resp.Unmarshal(data)

		log.WithFields(log.Fields{
			"op": strings.ToLower(ConvertOpCodeToString(resp.Op)),
			"id": resp.Id,
		}).Debug("Received Packet")

		var ch chan *Packet

		req, _ := t.requestBuffer.Get(resp.Id)

		ch = ((*PacketChannelTuple) (req)).Channel

		log.Debug("Sending Response to Channel")
		ch <- resp
		log.Debug("Closing Channel")
		close(ch)

		t.requestBuffer.Del(resp.Id)

		//log.WithFields(log.Fields{
		//	"requests": localRequests,
		//}).Debug("Local Requests")

		//if !ok {
		//	for req := range t.requestBuffer {
		//		pkt, channel := req.Packet, req.Channel
		//
		//		if pkt.Id == resp.Id {
		//			ch = channel
		//			break
		//		} else {
		//			localRequests[pkt.Id] = req
		//		}
		//	}
		//} else {
		//	ch = req.Channel
		//	delete(localRequests, resp.Id)
		//}


	}

}
