/*
 * Copyright 2020 Sourabh Bollapragada
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package communicator

import (
	"github.com/chemistry-sourabh/ifs/structure"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"net/url"
	"sync/atomic"
)

type FsWebSocketSender struct {
	msgId uint64
	conns map[string]*websocket.Conn
	send  map[string]chan []byte
	sent  map[string]chan *ReturnableMessage
}

func NewFsWebSocketSender() *FsWebSocketSender {
	return &FsWebSocketSender{
		msgId: 0,
		conns: make(map[string]*websocket.Conn),
		send:  make(map[string]chan []byte),
		sent:  make(map[string]chan *ReturnableMessage),
	}
}

func (fws *FsWebSocketSender) sendMessages(address string) {

	conn := fws.conns[address]
	send := fws.send[address]
	for data := range send {

		err := conn.WriteMessage(websocket.BinaryMessage, data)

		if err != nil {
			zap.L().Fatal("Failed to Send Message",
				zap.Error(err),
			)
		}

	}
}

func (fws *FsWebSocketSender) recvMessages(address string) {
	// TODO Make Global Dict
	sentMessages := make(map[uint64]*ReturnableMessage)

	conn := fws.conns[address]
	sent := fws.sent[address]
	for {
		messageType, data, err := conn.ReadMessage()

		if err != nil {
			zap.L().Fatal("Read Message Failed",
				zap.Error(err),
			)
			break
		}

		if messageType == websocket.BinaryMessage {
			reply := &structure.Reply{}

			err = proto.Unmarshal(data, reply)

			if err != nil {
				zap.L().Fatal("Failed to Unmarshal Message",
					zap.Error(err),
				)
			}

			zap.L().Debug("Received Message",
				zap.String("address", address),
				zap.Uint64("id", reply.Id),
				zap.Uint32("type", reply.PayloadType),
			)

			retMsg, ok := sentMessages[reply.Id]

			breakOut := false

			if ok {
				retMsg.RetChan <- reply
			} else {
				for {
					select {
					case sentMsg := <-sent:
						if sentMsg.Msg.Id == reply.Id {
							sentMsg.RetChan <- reply
							breakOut = true
							break
						} else {
							sentMessages[sentMsg.Msg.Id] = sentMsg
						}
					default:
						breakOut = true
						break
					}

					if breakOut {
						break
					}
				}
			}
		}
	}
}

func (fws *FsWebSocketSender) Connect(endpoints []string) {
	for i := 0; i < len(endpoints); i++ {
		u := url.URL{Scheme: "ws", Host: endpoints[i], Path: "/"}
		websocket.DefaultDialer.EnableCompression = true

		conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err != nil {
			zap.L().Fatal("Connection Handshake Failed",
				zap.Error(err),
			)
		}

		fws.conns[endpoints[i]] = conn
		fws.send[endpoints[i]] = make(chan []byte, structure.ChannelLength)
		fws.sent[endpoints[i]] = make(chan *ReturnableMessage, structure.ChannelLength)

		go fws.sendMessages(endpoints[i])
		go fws.recvMessages(endpoints[i])
	}
}

func (fws *FsWebSocketSender) Disconnect() {
	for addr, conn := range fws.conns {
		err := conn.Close()
		if err != nil {
			zap.L().Fatal("Failed to Close Connection",
				zap.String("address", addr),
				zap.Error(err),
			)
		}
	}
}

func (fws *FsWebSocketSender) SendRequest(payloadType uint32, address string, payload *structure.RequestPayload) (*structure.ReplyPayload, error) {
	msgId := atomic.AddUint64(&fws.msgId, 1)

	req := &structure.Request{
		Id:          msgId,
		PayloadType: payloadType,
		Payload:     payload,
	}

	retMsg := ReturnableMessage{
		Msg:     req,
		RetChan: make(chan *structure.Reply),
	}

	data, err := proto.Marshal(retMsg.Msg)

	if err != nil {
		return nil, err
	}

	fws.sent[address] <- &retMsg
	fws.send[address] <- data

	//TODO Timeout
	reply := <-retMsg.RetChan

	if reply.PayloadType == structure.ErrMessageCode {
		return nil, errors.New(reply.GetPayload().GetErrMsg().Error)
	}

	return reply.Payload, nil
}
