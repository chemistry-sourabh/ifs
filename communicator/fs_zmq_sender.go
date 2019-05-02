/*
 * Copyright 2019 Sourabh Bollapragada
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
	"github.com/chemistry-sourabh/ifs/structures"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
)

type ReturnableMessage struct {
	Msg     *structures.Request
	RetChan chan *structures.Reply
}

type FsZmqSender struct {
	msgId        uint64
	senderSocket *zmq.Socket
	recvSocket   *zmq.Socket
	stop         bool
	send         chan [][]byte
	sent         chan *ReturnableMessage
}

func NewFsZmqSender(address string) *FsZmqSender {

	zap.L().Debug("Creating Sender Socket",
		zap.String("address", address),
	)

	senderSocket, err := zmq.NewSocket(zmq.ROUTER)

	if err != nil {
		zap.L().Fatal("Failed to Create Socket",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	err = senderSocket.SetIdentity(address)

	if err != nil {
		zap.L().Fatal("Failed to Set Identity",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	err = senderSocket.SetLinger(0)

	if err != nil {
		zap.L().Fatal("Failed to Set Linger",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	zap.L().Debug("Creating Receiver Socket",
		zap.String("address", address),
	)

	recvSocket, err := zmq.NewSocket(zmq.ROUTER)

	if err != nil {
		zap.L().Fatal("Failed to Create Socket",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	err = recvSocket.SetIdentity(address)

	if err != nil {
		zap.L().Fatal("Failed to Set Identity",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	err = recvSocket.SetLinger(0)

	if err != nil {
		zap.L().Fatal("Failed to Set Linger",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	return &FsZmqSender{
		msgId:        0,
		senderSocket: senderSocket,
		recvSocket:   recvSocket,
		stop:         false,
		send:         make(chan [][]byte, structures.ChannelLength),
		sent:         make(chan *ReturnableMessage, structures.ChannelLength),
	}
}

//func (znm *FsZmqSender) processSocket() {
//	for {
//		time.Sleep(0)
//		if znm.stop {
//			break
//		}
//
//		znm.sendMessages()
//		znm.recvMessages()
//
//	}
//
//	znm.senderSocket.SetLinger(0)
//	znm.senderSocket.Close()
//}

func (fzs *FsZmqSender) sendMessages() {
	for data := range fzs.send {

		_, err := fzs.senderSocket.SendBytes(data[0], zmq.SNDMORE)

		if err != nil {
			zap.L().Fatal("Failed to Send Message",
				zap.Error(err),
			)
		}

		_, err = fzs.senderSocket.SendBytes(data[1], 0)

		if err != nil {
			zap.L().Fatal("Failed to Send Message",
				zap.Error(err),
			)
		}

	}
}

func (fzs *FsZmqSender) recvMessages() {

	sentMessages := make(map[uint64]*ReturnableMessage)

	for {

		frames, err := fzs.recvSocket.RecvMessageBytes(0)

		if err != nil {
			zap.L().Fatal("Failed to Receive Message",
				zap.Error(err),
			)
		}

		address := string(frames[0])
		data := frames[1]

		reply := &structures.Reply{}

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
				case sentMsg := <-fzs.sent:
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

// TODO Socket Destroy
func (fzs *FsZmqSender) Connect(remotes []string) {

	var recvRemotes []string

	for i := 0; i < len(remotes); i++ {
		recvRemotes = append(recvRemotes, GetOtherAddress(remotes[i]))
	}

	for i := 0; i < len(remotes); i++ {
		err := fzs.senderSocket.Connect("tcp://" + remotes[i])

		if err != nil {
			zap.L().Fatal("Failed to Connect",
				zap.String("address", remotes[i]),
			)
		}

		zap.L().Debug("Connected",
			zap.String("address", remotes[i]),
		)

		err = fzs.recvSocket.Connect("tcp://" + recvRemotes[i])

		if err != nil {
			zap.L().Fatal("Failed to Connect",
				zap.String("address", recvRemotes[i]),
			)
		}

		zap.L().Debug("Connected",
			zap.String("address", recvRemotes[i]),
		)

		time.Sleep(100 * time.Millisecond)
	}

	go fzs.sendMessages()
	go fzs.recvMessages()
}

func (fzs *FsZmqSender) Disconnect() {
	fzs.recvSocket.SetLinger(0)
	fzs.recvSocket.Close()
	fzs.senderSocket.SetLinger(0)
	fzs.senderSocket.Close()
}

func (fzs *FsZmqSender) SendRequest(payloadType uint32, address string, payload *structures.RequestPayload) (*structures.ReplyPayload, error) {

	msgId := atomic.AddUint64(&fzs.msgId, 1)

	req := &structures.Request{
		Id:          msgId,
		PayloadType: payloadType,
		Payload:     payload,
	}

	retMsg := ReturnableMessage{
		Msg:     req,
		RetChan: make(chan *structures.Reply),
	}

	data, err := proto.Marshal(retMsg.Msg)

	if err != nil {
		return nil, err
	}

	fzs.sent <- &retMsg
	fzs.send <- [][]byte{[]byte(address), data}

	//TODO Timeout
	reply := <-retMsg.RetChan

	return reply.Payload, nil
}
