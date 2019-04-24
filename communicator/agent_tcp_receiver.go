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
	"go.uber.org/zap"
	"gopkg.in/zeromq/goczmq.v4"
)

type AgentTcpReceiver struct {
	socket *goczmq.Sock
}

func NewAgentTcpReceiver() *AgentTcpReceiver {
	return &AgentTcpReceiver{}
}

func (znm *AgentTcpReceiver) RecvRequest() (uint64, uint32, string, *structures.RequestPayload, error) {
	zap.L().Debug("Listening For Requests")

	frames, err := znm.socket.RecvMessage()

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

func (znm *AgentTcpReceiver) SendReply(id uint64, payloadType uint32, address string, payload *structures.ReplyPayload) error {
	reply := &structures.Reply{
		Id:          id,
		PayloadType: payloadType,
		Payload:     payload,
	}

	data, err := proto.Marshal(reply)

	if err != nil {
		return err
	}

	err = znm.socket.SendMessage([][]byte{[]byte(address), data})

	if err != nil {
		return err
	}

	return nil
}

func (znm *AgentTcpReceiver) Bind(address string) error {

	zap.L().Info("Binding Socket",
		zap.String("address", address),
	)

	socket := goczmq.NewSock(goczmq.Router)
	socket.SetIdentity(address)

	_, err := socket.Bind("tcp://" + address)

	if err != nil {
		return err
	}

	znm.socket = socket

	return nil
}

func (znm *AgentTcpReceiver) Unbind() {
	zap.L().Debug("Destroyed Socket")
	znm.socket.Destroy()
}
