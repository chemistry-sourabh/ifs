package ifs

import (
	"net/url"
	"github.com/gorilla/websocket"
	"strings"
	"strconv"
	"sync/atomic"
	"sync"
	"go.uber.org/zap"
	"github.com/orcaman/concurrent-map"
)

type talker struct {
	// Should be map of hostname and port
	//IdCounters    map[string]*uint64
	IdCounters cmap.ConcurrentMap
	//Pools         map[string]*FsConnectionPool
	Pools         cmap.ConcurrentMap
	RequestBuffer cmap.ConcurrentMap
}

var (
	talkerInstance *talker
	talkerOnce     sync.Once
)

func Talker() *talker {
	talkerOnce.Do(func() {
		talkerInstance = &talker{
			IdCounters:    cmap.New(),
			Pools:         cmap.New(),
			RequestBuffer: cmap.New(),
		}
	})

	return talkerInstance

}

func (t talker) getPool(hostname string) *FsConnectionPool {
	val, _ := t.Pools.Get(hostname)
	return val.(*FsConnectionPool)
}

func (t *talker) getIdCounter(hostname string) *uint64 {
	val, _ := t.IdCounters.Get(hostname)
	return val.(*uint64)
}

func (t *talker) Startup(remoteRoots []*RemoteRoot, poolCount int) {

	for _, remoteRoot := range remoteRoots {

		idCounter := uint64(0)
		t.IdCounters.Set(remoteRoot.Hostname, &idCounter)
		t.Pools.Set(remoteRoot.Hostname, newFsConnectionPool())
		t.mountRemoteRoot(remoteRoot, poolCount)
	}
}

func (t *talker) mountRemoteRoot(remoteRoot *RemoteRoot, poolCount int) {

	u := url.URL{Scheme: "ws", Host: remoteRoot.Address(), Path: "/"}

	for i := 0; i < poolCount; i++ {

		c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)

		if err != nil {
			zap.L().Fatal("Connection Handshake Failed",
				zap.Error(err),
			)
		}

		t.getPool(remoteRoot.Hostname).Append(c)

		index := uint8(t.getPool(remoteRoot.Hostname).Len() - 1)
		go t.processSendingChannel(remoteRoot.Hostname, index)
		go t.processIncomingMessages(remoteRoot.Hostname, index)

	}

	payload := &WatchInfo{
		Paths: remoteRoot.Paths,
	}

	t.sendRequest(WatchDirRequest, remoteRoot.Hostname, payload)

}

func (t *talker) sendRequest(opCode uint8, hostname string, payload Payload) *Packet {

	respChannel := make(chan *Packet)

	req := &Packet{
		Op:   opCode,
		Data: payload,
	}

	t.getPool(hostname).SendingChannels[GetRandomIndex(t.getPool(hostname).Len())] <- &PacketChannelTuple{
		req,
		respChannel,
	}

	return <-respChannel
}

func GetMapKey(hostname string, connId uint8, id uint64) string {
	return strings.Join([]string{hostname, strconv.FormatInt(int64(connId), 10), strconv.FormatInt(int64(id), 10)}, "_")
}

func (t *talker) processSendingChannel(hostname string, index uint8) {

	zap.L().Info("Starting Egress Channel Processor",
		zap.String("hostname", hostname),
		zap.Uint8("index", index),
	)

	for req := range t.getPool(hostname).SendingChannels[index] {

		pkt, _ := req.Packet, req.Channel

		pkt.ConnId = index
		pkt.Id = atomic.AddUint64(t.getIdCounter(hostname), 1)

		zap.L().Debug("Sending Packet",
			zap.String("hostname", hostname),
			zap.Uint8("index", index),
			zap.String("op", strings.ToLower(ConvertOpCodeToString(pkt.Op))),
			zap.Uint8("conn_id", pkt.ConnId),
			zap.Uint64("id", pkt.Id),
		)

		t.RequestBuffer.Set(GetMapKey(hostname, pkt.ConnId, pkt.Id), req)

		data, _ := pkt.Marshal()
		err := t.getPool(hostname).Connections[index].WriteMessage(websocket.BinaryMessage, data)
		if err != nil {
			zap.L().Fatal("Write Message Failed",
				zap.Error(err),
			)
		}

	}
}

func (t *talker) processIncomingMessages(hostname string, index uint8) {

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

		_, data, err := t.getPool(hostname).Connections[index].ReadMessage()

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

			req, _ := t.RequestBuffer.Get(GetMapKey(hostname, packet.ConnId, packet.Id))

			ch = req.(*PacketChannelTuple).Channel

			ch <- packet
			close(ch)

			t.RequestBuffer.Remove(GetMapKey(hostname, packet.ConnId, packet.Id))

		} else {
			go t.processRequest(hostname, packet)
		}
	}
}

func (t *talker) processRequest(hostname string, packet *Packet) {

	switch packet.Op {
	case AttrUpdateRequest:
		attrUpdateInfo := packet.Data.(*AttrUpdateInfo)
		Ifs().UpdateAttr(hostname, attrUpdateInfo)
	}
}
