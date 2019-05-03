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
	"time"
)

type AgentZmqReceiver struct {
	senderSocket *zmq.Socket
	recvSocket   *zmq.Socket
	send         chan [][]byte
}

func NewAgentZmqReceiver() *AgentZmqReceiver {
	return &AgentZmqReceiver{
		send: make(chan [][]byte, structures.ChannelLength),
	}
}

func (azr *AgentZmqReceiver) sendMessages() {
	for data := range azr.send {
		_, err := azr.senderSocket.SendBytes(data[0], zmq.SNDMORE)

		if err != nil {
			zap.L().Fatal("Failed to Send Reply",
				zap.Error(err),
			)
		}

		_, err = azr.senderSocket.SendMessage(data[1], 0)

		if err != nil {
			zap.L().Fatal("Failed to Send Reply",
				zap.Error(err),
			)
		}

	}
}

func (azr *AgentZmqReceiver) RecvRequest() (uint64, uint32, string, *structures.RequestPayload, error) {
	frames, err := azr.recvSocket.RecvMessageBytes(0)

	if err != nil {
		return 0, 0, "", nil, err
	}

	address := string(frames[0])
	data := frames[1]

	request := &structures.Request{}

	err = proto.Unmarshal(data, request)

	if err != nil {
		return 0, 0, "", nil, err
	}

	zap.L().Debug("Received Message",
		zap.String("address", address),
		zap.Uint64("id", request.Id),
		zap.Uint32("type", request.PayloadType),
	)

	return request.Id, request.PayloadType, address, request.Payload, nil
}

func (azr *AgentZmqReceiver) SendReply(id uint64, payloadType uint32, address string, payload *structures.ReplyPayload) error {
	reply := &structures.Reply{
		Id:          id,
		PayloadType: payloadType,
		Payload:     payload,
	}

	data, err := proto.Marshal(reply)

	if err != nil {
		return err
	}

	azr.send <- [][]byte{[]byte(address), data}

	return nil
}

func (azr *AgentZmqReceiver) Bind(address string) error {

	zap.L().Info("Binding Socket",
		zap.String("address", address),
	)

	recvSocket, err := zmq.NewSocket(zmq.ROUTER)

	if err != nil {
		return err
	}

	err = recvSocket.SetIdentity(address)

	if err != nil {
		return err
	}

	err = recvSocket.Bind("tcp://" + address)

	if err != nil {
		return err
	}

	senderAddress := GetOtherAddress(address)

	senderSocket, err := zmq.NewSocket(zmq.ROUTER)

	if err != nil {
		zap.L().Fatal("Agent Couldn't Create Socket",
			zap.String("address", senderAddress),
			zap.Error(err),
		)
	}

	err = senderSocket.SetIdentity(senderAddress)

	if err != nil {
		zap.L().Fatal("Agent Couldn't Set Identity",
			zap.String("address", senderAddress),
			zap.Error(err),
		)
	}

	err = senderSocket.Bind("tcp://" + senderAddress)

	if err != nil {
		zap.L().Fatal("Agent Couldn't Bind to Socket",
			zap.String("address", senderAddress),
			zap.Error(err),
		)
	}

	azr.senderSocket = senderSocket
	azr.recvSocket = recvSocket

	time.Sleep(100 * time.Millisecond)

	go azr.sendMessages()

	return nil
}

func (azr *AgentZmqReceiver) Unbind() {
	zap.L().Debug("Destroyed Socket")
	azr.recvSocket.SetLinger(0)
	azr.recvSocket.Close()
	azr.senderSocket.SetLinger(0)
	azr.senderSocket.Close()
}
