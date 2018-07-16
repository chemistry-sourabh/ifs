package ifs

import (
	"net/url"
	"github.com/gorilla/websocket"
	"strings"
	"strconv"
	"sync/atomic"
	"sync"
	"go.uber.org/zap"
)

type Talker struct {
	// Should be map of hostname and port
	Ifs           *Ifs
	IdCounters    map[string]*uint64
	Pools         map[string]*FsConnectionPool
	RequestBuffer *sync.Map
}

func NewTalker(Ifs *Ifs) *Talker {
	return &Talker{
		Ifs:           Ifs,
		RequestBuffer: &sync.Map{},
		IdCounters:    make(map[string]*uint64),
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
			zap.L().Fatal("Connection Handshake Failed",
				zap.Error(err),
			)
		}

		t.Pools[remoteRoot.Hostname].Append(c)

		index := uint8(t.Pools[remoteRoot.Hostname].Len() - 1)
		go t.processSendingChannel(remoteRoot.Hostname, index)
		go t.processIncomingMessages(remoteRoot.Hostname, index)

	}

	payload := &WatchInfo{
		Paths: remoteRoot.Paths,
	}

	t.sendRequest(WatchDirRequest, remoteRoot.Hostname, payload)

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

	zap.L().Info("Starting Egress Channel Processor",
		zap.String("hostname", hostname),
		zap.Uint8("index", index),
	)

	for req := range t.Pools[hostname].SendingChannels[index] {

		pkt, _ := req.Packet, req.Channel

		pkt.ConnId = index
		pkt.Id = atomic.AddUint64(t.IdCounters[hostname], 1)

		zap.L().Debug("Sending Packet",
			zap.String("hostname", hostname),
			zap.Uint8("index", index),
			zap.String("op", strings.ToLower(ConvertOpCodeToString(pkt.Op))),
			zap.Uint8("conn_id", pkt.ConnId),
			zap.Uint64("id", pkt.Id),
		)

		t.RequestBuffer.Store(GetMapKey(hostname, pkt.ConnId, pkt.Id), req)

		data, _ := pkt.Marshal()
		err := t.Pools[hostname].Connections[index].WriteMessage(websocket.BinaryMessage, data)
		if err != nil {
			zap.L().Fatal("Write Message Failed",
				zap.Error(err),
			)
		}

	}
}

func (t *Talker) processIncomingMessages(hostname string, index uint8) {

	zap.L().Info("Starting Ingress Message Processor",
		zap.String("hostname", hostname),
		zap.Uint8("index", index),
	)

	for {

		packet := &Packet{}

		zap.L().Debug("Listening For Packet",
			zap.String("hostname", hostname),
			zap.Uint8("index", index),
		)

		_, data, err := t.Pools[hostname].Connections[index].ReadMessage()

		if err != nil {
			zap.L().Fatal("Read Message Failed",
				zap.Error(err),
			)
			break
		}

		packet.Unmarshal(data)

		zap.L().Debug("Received Packet",
			zap.String("hostname", hostname),
			zap.Uint8("index", index),
			zap.String("op", strings.ToLower(ConvertOpCodeToString(packet.Op))),
			zap.Uint8("conn_id", packet.ConnId),
			zap.Bool("request", packet.IsRequest()),
			zap.Uint64("id", packet.Id),
		)

		if !packet.IsRequest() {

			var ch chan *Packet

			req, _ := t.RequestBuffer.Load(GetMapKey(hostname, packet.ConnId, packet.Id))

			ch = req.(*PacketChannelTuple).Channel

			ch <- packet
			close(ch)

			t.RequestBuffer.Delete(GetMapKey(hostname, packet.ConnId, packet.Id))

		} else {
			go t.processRequest(hostname, packet)
		}
	}
}

func (t *Talker) processRequest(hostname string, packet *Packet) {

	switch packet.Op {
	case AttrUpdateRequest:
		attrUpdateInfo := packet.Data.(*AttrUpdateInfo)
		t.Ifs.UpdateAttr(hostname, attrUpdateInfo)
	}
}
