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
	Ifs        *Ifs
	IdCounters map[string] *uint64
	//IdCounters    map[uint8]uint64
	Pools map[string]*FsConnectionPool
	//Pool          *FsConnectionPool
	RequestBuffer *FastMap // TODO Change To sync.Map
	//RequestBuffer        chan *PacketChannelTuple // One Receiver for each pool ?
	//egressRequestChannel chan *PacketChannelTuple
}

func NewTalker(Ifs *Ifs) *Talker {
	return &Talker{
		Ifs:           Ifs,
		RequestBuffer: NewFastMap(),
		IdCounters:    make(map[string] *uint64),
		Pools:         make(map[string]*FsConnectionPool),
	}
}

func (t *Talker) Startup(remoteRoots []*RemoteRoot, poolCount int) {

	for _, remoteRoot := range remoteRoots {

		idCounter := uint64(0)
		t.IdCounters[remoteRoot.Hostname] = &idCounter
		t.Pools[remoteRoot.Hostname] = newFsConnectionPool()
		t.mountRemoteRoot(remoteRoot, poolCount)
	}
}

func (t *Talker) mountRemoteRoot(remoteRoot *RemoteRoot, poolCount int) {

	u := url.URL{Scheme: "ws", Host: remoteRoot.Address(), Path: "/"}

	for i := 0; i < poolCount; i++ {

		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

		if err != nil {
			log.Fatal(err)
		}

		t.Pools[remoteRoot.Hostname].Append(c)

		index := uint8(t.Pools[remoteRoot.Hostname].Len() - 1)
		go t.processSendingChannel(remoteRoot.Hostname, index)
		go t.processIncomingMessages(remoteRoot.Hostname, index)

	}
}

func (t *Talker) sendRequest(opCode uint8, hostname string, payload Payload) *Packet {

	respChannel := make(chan *Packet)

	req := &Packet{
		Op:   opCode,
		Data: payload,
	}

	t.Pools[hostname].SendingChannels[GetRandomIndex(t.Pools[hostname].Len())] <- &PacketChannelTuple{
		req,
		respChannel,
	}

	return <-respChannel
}

func GetMapKey(hostname string, connId uint8, id uint64) string {
	return strings.Join([]string{hostname, strconv.FormatInt(int64(connId), 10), strconv.FormatInt(int64(id), 10)}, "_")
}

func (t *Talker) processSendingChannel(hostname string, index uint8) {

	log.Info("Starting Egress Channel Processor")

	log.WithFields(log.Fields{
		"index": index,
		"hostname": hostname,
		"Pools": t.Pools,
		"Pool": t.Pools[hostname].SendingChannels,
	}).Debug("Info")
	for req := range t.Pools[hostname].SendingChannels[index] {

		pkt, _ := req.Packet, req.Channel

		pkt.ConnId = index
		pkt.Id = atomic.AddUint64(t.IdCounters[hostname], 1)
		//pkt.Id = t.IdCounters[index]
		//t.IdCounters[index]++

		log.WithFields(log.Fields{
			"op":      strings.ToLower(ConvertOpCodeToString(pkt.Op)),
			"id":      pkt.Id,
			"conn_id": pkt.ConnId,
		}).Debug("Sending Packet")

		t.RequestBuffer.Set(GetMapKey(hostname, pkt.ConnId, pkt.Id), req)

		data, _ := pkt.Marshal()
		err := t.Pools[hostname].Connections[index].WriteMessage(websocket.BinaryMessage, data)
		if err != nil {
			log.Fatal(err)
		}

	}
}

func (t *Talker) processIncomingMessages(hostname string, index uint8) {
	log.Info("Starting Incoming Message Processor")

	for {

		packet := &Packet{}

		log.Debug("Listening for Packet")

		_, data, err := t.Pools[hostname].Connections[index].ReadMessage()

		if err != nil {
			log.Fatal(err)
			break
		}

		packet.Unmarshal(data)

		log.WithFields(log.Fields{
			"op":      strings.ToLower(ConvertOpCodeToString(packet.Op)),
			"request": packet.IsRequest(),
			"id":      packet.Id,
			"conn_id": packet.ConnId,
		}).Debug("Received Packet")

		if !packet.IsRequest() {

			var ch chan *Packet

			req, _ := t.RequestBuffer.Load(GetMapKey(packet.ConnId, packet.Id))

			ch = req.(*PacketChannelTuple).Channel

			log.Debug("Sending Response to Channel")
			ch <- packet
			log.Debug("Closing Channel")
			close(ch)
			log.Debug("Closed Channel")

			t.RequestBuffer.Delete(GetMapKey(packet.ConnId, packet.Id))

		} else {
			go t.processRequest(packet)
		}
	}
}

func (t *Talker) processRequest(packet *Packet) {
	// Blah Blah
}
