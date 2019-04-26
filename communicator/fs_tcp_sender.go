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

type FsTcpSender struct {
	msgId  uint64
	socket *zmq.Socket
	stop   bool
	recv   chan [][]byte
	send   chan [][]byte
	sent   chan *ReturnableMessage
}

func NewFsTcpSender(address string) *FsTcpSender {
	sock, err := zmq.NewSocket(zmq.ROUTER)

	if err != nil {
		zap.L().Fatal("Failed to Create Socket",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	err = sock.SetIdentity(address)

	if err != nil {
		zap.L().Fatal("Failed to Set Identity",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	return &FsTcpSender{
		msgId:  0,
		socket: sock,
		stop: false,
		recv:   make(chan [][]byte, structures.ChannelLength),
		send:   make(chan [][]byte, structures.ChannelLength),
		sent:   make(chan *ReturnableMessage, structures.ChannelLength),
	}
}

func (znm *FsTcpSender) processSocket() {
	for {
		time.Sleep(0)
		if znm.stop {
			break
		}

		znm.sendMessages()
		znm.recvMessages()

	}


	znm.socket.SetLinger(0)
	znm.socket.Close()
}

func (znm *FsTcpSender) sendMessages() {
	for {

		select {

		case data := <-znm.send:
			_, err := znm.socket.SendBytes(data[0], zmq.SNDMORE)

			if err != nil {
				zap.L().Error("Failed to Send Message",
					zap.Error(err),
				)
			}

			_, err = znm.socket.SendBytes(data[1], 0)

			if err != nil {
				zap.L().Error("Failed to Send Message",
					zap.Error(err),
				)
			}

		default:
			return
		}
	}
}

func (znm *FsTcpSender) recvMessages() {

	//sentMessages := make(map[uint64]*ReturnableMessage)

	for {

		frames, err := znm.socket.RecvMessageBytes(zmq.DONTWAIT)

		if err != nil {
			zap.L().Error("Failed to Receive Message",
				zap.Error(err),
			)
			break
		}

		znm.recv <- frames
		//address := string(frames[0])
		//data := frames[1]
		//
		//reply := &structures.Reply{}
		//
		//err = proto.Unmarshal(data, reply)
		//
		//if err != nil {
		//	zap.L().Error("Failed to Unmarshal Message",
		//		zap.Error(err),
		//	)
		//	continue
		//}
		//
		//zap.L().Debug("Received Message",
		//	zap.String("address", address),
		//	zap.Uint64("id", reply.Id),
		//	zap.Uint32("type", reply.PayloadType),
		//)


		//retMsg, ok := sentMessages[reply.Id]
		//
		//breakOut := false
		//
		//if ok {
		//	retMsg.RetChan <- reply
		//} else {
		//	for {
		//		select {
		//		case sentMsg := <-znm.sent:
		//			if sentMsg.Msg.Id == reply.Id {
		//				sentMsg.RetChan <- reply
		//				breakOut = true
		//				break
		//			} else {
		//				sentMessages[sentMsg.Msg.Id] = sentMsg
		//			}
		//		default:
		//			breakOut = true
		//			break
		//		}
		//
		//		if breakOut {
		//			break
		//		}
		//	}
		//}
	}
}

func (znm *FsTcpSender) processRecvMessages() {

	sentMessages := make(map[uint64]*ReturnableMessage)

	for frames := range znm.recv {
		address := string(frames[0])
		data := frames[1]

		reply := &structures.Reply{}

		err := proto.Unmarshal(data, reply)

		if err != nil {
			zap.L().Error("Failed to Unmarshal Message",
				zap.Error(err),
			)
			continue
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
				case sentMsg := <-znm.sent:
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
func (znm *FsTcpSender) Connect(remotes []string) {
	for i := 0; i < len(remotes); i++ {
		err := znm.socket.Connect("tcp://" + remotes[i])

		if err != nil {
			//return err
		}

		zap.L().Debug("Connected",
			zap.String("Endpoint", remotes[i]),
		)

		time.Sleep(100 * time.Millisecond)
	}

	go znm.processRecvMessages()
	go znm.processSocket()
	//go znm.sendMessages()
	//go znm.recvMessages()
}

func (znm *FsTcpSender) Disconnect() {
	znm.stop = true
}

func (znm *FsTcpSender) SendRequest(payloadType uint32, address string, payload *structures.RequestPayload) (*structures.ReplyPayload, error) {

	msgId := atomic.AddUint64(&znm.msgId, 1)

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

	znm.sent <- &retMsg
	znm.send <- [][]byte{[]byte(address), data}

	//TODO Timeout
	reply := <-retMsg.RetChan

	return reply.Payload, nil
}
