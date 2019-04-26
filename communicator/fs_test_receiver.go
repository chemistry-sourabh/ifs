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
	stop   bool
	socket *zmq.Socket
}

func (znm *FsTestReceiver) recvSocket() {
	zap.L().Info("Listening For Messages")

	for {
		frames, err := znm.socket.RecvMessageBytes(zmq.DONTWAIT)

		if err != nil {
			zap.L().Error("Listening Failed",
				zap.Error(err),
			)
			if znm.stop {
				break
			}
			time.Sleep(0)
			continue
		}

		address := string(frames[0])
		data := frames[1]

		request := &structures.Request{}

		err = proto.Unmarshal(data, request)

		if err != nil {
			zap.L().Error("Unmarshalling Failed",
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
		reply.PayloadType = structures.FileMessageCode

		data, err = proto.Marshal(reply)

		if err != nil {
			zap.L().Error("Couldnt Marshall",
				zap.Error(err),
			)
		}

		_, err = znm.socket.SendBytes(frames[0], zmq.SNDMORE)

		if err != nil {
			zap.L().Error("Couldn't Send",
				zap.Error(err),
			)
		}

		_, err = znm.socket.SendBytes(data, 0)

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
	znm.socket.SetLinger(0)
	znm.socket.Close()
}

func (znm *FsTestReceiver) Startup(address string) {

	zap.L().Info("Starting Agent",
		zap.String("address", address),
	)

	socket, err := zmq.NewSocket(zmq.ROUTER)

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

	err = socket.Bind("tcp://" + address)

	if err != nil {
		zap.L().Fatal("Agent Couldn't Bind to Socket",
			zap.String("address", address),
			zap.Error(err),
		)
	}

	znm.socket = socket

	go znm.recvSocket()
}

func (znm *FsTestReceiver) Disconnect() {
	znm.stop = true
}
