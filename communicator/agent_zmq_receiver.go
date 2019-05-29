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
	"github.com/chemistry-sourabh/ifs/structure"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

type AgentZmqReceiver struct {
	ctx           *zmq.Context
	idAddress     sync.Map
	senderAddress string
	recvAddress   string
	send          chan [][]byte
	recv          chan *structure.Request
}

func NewAgentZmqReceiver() *AgentZmqReceiver {

	ctx, err := zmq.NewContext()

	if err != nil {
		zap.L().Fatal("Failed to Create Context",
			zap.Error(err),
		)
	}

	return &AgentZmqReceiver{
		ctx:       ctx,
		idAddress: sync.Map{},
		send:      make(chan [][]byte, structure.ChannelLength),
		recv:      make(chan *structure.Request, structure.ChannelLength),
	}
}

func (azr *AgentZmqReceiver) createSocket(address string) *zmq.Socket {
	socket, err := azr.ctx.NewSocket(zmq.ROUTER)

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

	err = socket.Bind("tcp://" + address)

	if err != nil {
		zap.L().Fatal("Agent Couldn't Bind to Socket",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	time.Sleep(100 * time.Millisecond)

	return socket
}

func (azr *AgentZmqReceiver) sendMessages() {

	senderSocket := azr.createSocket(azr.senderAddress)

	for data := range azr.send {
		_, err := senderSocket.SendMessage(data)

		if err != nil {
			zap.L().Fatal("Failed to Send Reply",
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

func (azr *AgentZmqReceiver) recvMessages() {

	recvSocket := azr.createSocket(azr.recvAddress)

	for {
		frames, err := recvSocket.RecvMessageBytes(0)

		if err != nil {

			if err.Error() == "Context was terminated" {
				zap.L().Debug("Context was terminated")
				break
			}

			zap.L().Fatal("Listening Failed",
				zap.Error(err),
			)
		}

		address := string(frames[0])
		data := frames[1]

		request := &structure.Request{}

		err = proto.Unmarshal(data, request)

		if err != nil {
			zap.L().Fatal("Unmarshalling Failed",
				zap.Error(err),
			)
		}

		zap.L().Debug("Received Message",
			zap.String("address", address),
			zap.Uint64("id", request.Id),
			zap.Uint32("type", request.PayloadType),
		)

		azr.idAddress.Store(request.Id, address)

		azr.recv <- request

	}

	err := recvSocket.Close()

	if err != nil {
		zap.L().Fatal("Failed to Close Recv Socket",
			zap.Error(err),
		)
	}

}

func (azr *AgentZmqReceiver) RecvRequest() (uint64, uint32, *structure.RequestPayload, error) {

	request, ok := <-azr.recv

	if ok {
		return request.Id, request.PayloadType, request.Payload, nil
	} else {
		return 0, 0, nil, errors.New("Channel Closed")
	}
}

func (azr *AgentZmqReceiver) SendReply(id uint64, payloadType uint32, payload *structure.ReplyPayload) error {
	reply := &structure.Reply{
		Id:          id,
		PayloadType: payloadType,
		Payload:     payload,
	}

	data, err := proto.Marshal(reply)

	if err != nil {
		return err
	}

	val, _ := azr.idAddress.Load(id)
	address := val.(string)

	azr.idAddress.Delete(id)

	azr.send <- [][]byte{[]byte(address), data}

	return nil
}

func (azr *AgentZmqReceiver) Bind(address string) error {

	azr.recvAddress = address
	azr.senderAddress = GetOtherAddress(address)

	go azr.recvMessages()
	go azr.sendMessages()

	return nil
}

func (azr *AgentZmqReceiver) Unbind() {
	close(azr.send)
	close(azr.recv)
	err := azr.ctx.Term()

	if err != nil {
		zap.L().Fatal("Failed to Destroy Context",
			zap.Error(err),
		)
	}
}
