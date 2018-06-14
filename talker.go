package ifs

import (
	"net/url"
	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
	"strings"
	"strconv"
	"sync/atomic"
)

type Talker struct {
	// Should be map of hostname and port
	Ifs           *Ifs
	IdCounter	  uint64
	//IdCounters    map[uint8]uint64
	Pool          *FsConnectionPool
	RequestBuffer *FastMap
	//RequestBuffer        chan *PacketChannelTuple // One Receiver for each pool ?
	//egressRequestChannel chan *PacketChannelTuple
}

func NewTalker(Ifs *Ifs) *Talker {
	return &Talker{
		Ifs:           Ifs,
		RequestBuffer: NewFastMap(),
		//IdCounters:    make(map[uint8]uint64),
		Pool:          newFsConnectionPool(),
	}
}

func (t *Talker) Startup(address string, poolCount int) {
	t.mountRemoteRoot(address, poolCount)
}

func (t *Talker) mountRemoteRoot(address string, poolCount int) {

	u := url.URL{Scheme: "ws", Host: address, Path: "/"}

	for i := 0; i < poolCount; i++ {

		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

		if err != nil {
			log.Fatal(err)
		}

		t.Pool.Append(c)

		index := uint8(len(t.Pool.Connections) - 1)
		go t.processSendingChannel(index)
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

func GetMapKey(connId uint8, id uint64) string {
	return strings.Join([]string{strconv.FormatInt(int64(connId), 10), strconv.FormatInt(int64(id), 10)},"_")
}

func (t *Talker) processSendingChannel(index uint8) {

	log.Info("Starting Egress Channel Processor")

	for req := range t.Pool.SendingChannels[index] {

		pkt, _ := req.Packet, req.Channel

		pkt.ConnId = index
		pkt.Id = atomic.AddUint64(&t.IdCounter, 1)
		//pkt.Id = t.IdCounters[index]
		//t.IdCounters[index]++

		log.WithFields(log.Fields{
			"op": strings.ToLower(ConvertOpCodeToString(pkt.Op)),
			"id": pkt.Id,
			"conn_id": pkt.ConnId,
		}).Debug("Sending Packet")

		t.RequestBuffer.Set(GetMapKey(pkt.ConnId, pkt.Id), req)

		data, _ := pkt.Marshal()
		err := t.Pool.Connections[index].WriteMessage(websocket.BinaryMessage, data)
		if err != nil {
			log.Fatal(err)
		}

	}
}

func (t *Talker) processIncomingMessages(index uint8) {
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
			"conn_id": resp.ConnId,
		}).Debug("Received Packet")

		var ch chan *Packet

		req, _ := t.RequestBuffer.Load(GetMapKey(resp.ConnId, resp.Id))

		ch = req.(*PacketChannelTuple).Channel

		log.Debug("Sending Response to Channel")
		ch <- resp
		log.Debug("Closing Channel")
		close(ch)
		log.Debug("Closed Channel")

		t.RequestBuffer.Delete(GetMapKey(resp.ConnId, resp.Id))

	}

}
