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
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"sync/atomic"
	"time"
)

type ReturnableMessage struct {
	Msg     *structures.Request
	RetChan chan *structures.Reply
}

type FsZmqSender struct {
	msgId             uint64
	ctx               *zmq.Context
	clientAddress     string
	senderEndpoints   []string
	receiverEndpoints []string
	//senderSocket      *zmq.Socket
	//recvSocket        *zmq.Socket
	send chan [][]byte
	sent chan *ReturnableMessage
}

func NewFsZmqSender(address string) *FsZmqSender {

	ctx, err := zmq.NewContext()

	if err != nil {
		zap.L().Fatal("Failed to Create Context",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	return &FsZmqSender{
		msgId:         0,
		ctx:           ctx,
		clientAddress: address,
		send:          make(chan [][]byte, structures.ChannelLength),
		sent:          make(chan *ReturnableMessage, structures.ChannelLength),
	}
}

func (fzs *FsZmqSender) createSocket(address string, endpoints []string) *zmq.Socket {
	socket, err := fzs.ctx.NewSocket(zmq.ROUTER)

	if err != nil {
		zap.L().Fatal("Agent Couldn't Create Socket",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	err = socket.SetIdentity(address)

	if err != nil {
		zap.L().Fatal("Agent Couldn't Set Identity",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	err = socket.SetLinger(0)

	if err != nil {
		zap.L().Fatal("Failed to Set Linger",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	for i := 0; i < len(endpoints); i++ {
		err := socket.Connect(endpoints[i])

		if err != nil {
			zap.L().Fatal("Failed to Connect",
				zap.String("address", endpoints[i]),
			)
		}

		zap.L().Debug("Connected",
			zap.String("address", endpoints[i]),
		)

		time.Sleep(100 * time.Millisecond)
	}

	return socket
}

func (fzs *FsZmqSender) sendMessages() {

	senderSocket := fzs.createSocket(fzs.clientAddress, fzs.senderEndpoints)

	for data := range fzs.send {

		_, err := senderSocket.SendMessage(data)

		if err != nil {
			zap.L().Fatal("Failed to Send Message",
				zap.Error(err),
			)
		}

	}

	err := senderSocket.Close()

	if err != nil {
		zap.L().Fatal("Failed to Close Sender Socket",
			zap.Error(err),
		)
	}
}

func (fzs *FsZmqSender) recvMessages() {

	sentMessages := make(map[uint64]*ReturnableMessage)

	recvSocket := fzs.createSocket(fzs.clientAddress, fzs.receiverEndpoints)

	for {

		frames, err := recvSocket.RecvMessageBytes(0)

		if err != nil {

			if err.Error() == "Context was terminated" {
				break
			}

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

	err := recvSocket.Close()

	if err != nil {
		zap.L().Fatal("Failed to Close Recv Socket",
			zap.Error(err),
		)
	}
}

// TODO Socket Destroy
func (fzs *FsZmqSender) Connect(remotes []string) {

	var recvEndpoints []string
	var senderEndpoints []string

	for i := 0; i < len(remotes); i++ {
		recvEndpoints = append(recvEndpoints, "tcp://"+GetOtherAddress(remotes[i]))
	}

	for i := 0; i < len(remotes); i++ {
		senderEndpoints = append(senderEndpoints, "tcp://"+remotes[i])
	}

	fzs.senderEndpoints = senderEndpoints
	fzs.receiverEndpoints = recvEndpoints

	go fzs.sendMessages()
	go fzs.recvMessages()
}

func (fzs *FsZmqSender) Disconnect() {
	close(fzs.send)
	err := fzs.ctx.Term()

	if err != nil {
		zap.L().Fatal("Failed to Destroy Context",
			zap.Error(err),
		)
	}
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

	if reply.PayloadType == structures.ErrMessageCode {
		return nil, errors.New(reply.GetPayload().GetErrMsg().Error)
	}

	return reply.Payload, nil
}
