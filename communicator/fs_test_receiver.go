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

type FsTestReceiver struct {
	ctx           *zmq.Context
	senderAddress string
	recvAddress   string
}

func (ftr *FsTestReceiver) createSocket(address string) *zmq.Socket {
	socket, err := ftr.ctx.NewSocket(zmq.ROUTER)

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

func (ftr *FsTestReceiver) recvMessages() {
	zap.L().Info("Creating Sockets")

	recvSocket := ftr.createSocket(ftr.recvAddress)
	senderSocket := ftr.createSocket(ftr.senderAddress)

	zap.L().Info("Listening For Messages")

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

		request := &structures.Request{}

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

		reply := &structures.Request{}

		reply.Id = request.Id
		reply.PayloadType = structures.DataMessageCode

		data, err = proto.Marshal(reply)

		if err != nil {
			zap.L().Error("Couldnt Marshall",
				zap.Error(err),
			)
		}

		_, err = senderSocket.SendMessage([][]byte{frames[0], data})

		if err != nil {
			zap.L().Error("Couldn't Send",
				zap.Error(err),
			)
		}

		zap.L().Debug("Sent Message",
			zap.String("address", address),
			zap.Uint64("Id", reply.Id),
			zap.Uint32("Type", reply.PayloadType),
		)
	}

	err := recvSocket.Close()

	if err != nil {
		zap.L().Fatal("Failed to Close Socket",
			zap.Error(err),
		)
	}

	err = senderSocket.Close()

	if err != nil {
		zap.L().Fatal("Failed to Close Socket",
			zap.Error(err),
		)
	}
}

func (ftr *FsTestReceiver) Bind(address string) {

	ctx, err := zmq.NewContext()

	if err != nil {
		zap.L().Fatal("Failed creating context",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	zap.L().Info("Starting Agent",
		zap.String("address", address),
	)


	ftr.ctx = ctx
	ftr.recvAddress = address
	ftr.senderAddress = GetOtherAddress(address)

	go ftr.recvMessages()
}

func (ftr *FsTestReceiver) Unbind() {

	err := ftr.ctx.Term()

	if err != nil {
		zap.L().Fatal("Failed to Context Terminate",
			zap.Error(err),
		)
	}

}
