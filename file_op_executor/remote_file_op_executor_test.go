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

package file_op_executor

import (
	"github.com/chemistry-sourabh/ifs/communicator"
	"github.com/chemistry-sourabh/ifs/ifstest"
	"github.com/chemistry-sourabh/ifs/structures"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"math/rand"
	"path"
	"testing"
	"time"
)

func TestRemoteFileOpExecutor_FetchFile(t *testing.T) {
	ifstest.SetupLogger()

	clientAddress := "127.0.0.1:5000"
	agentAddress := "127.0.0.1:5012"
	fileName := "file1"

	ifstest.CreateTempFile(fileName)
	fileData := ifstest.WriteDummyData(fileName, 1000)

	atr := communicator.NewAgentZmqReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	fm := &structures.FetchMessage{
		Path: path.Join("/tmp", fileName),
	}

	requestPayload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_FetchMsg{
			FetchMsg: fm,
		},
	}

	senderSocket, err := zmq.NewSocket(zmq.ROUTER)
	ifstest.Ok(t, err)

	err = senderSocket.SetIdentity(clientAddress)
	ifstest.Ok(t, err)

	err = senderSocket.Connect("tcp://" + agentAddress)
	ifstest.Ok(t, err)

	recvSocket, err := zmq.NewSocket(zmq.ROUTER)
	ifstest.Ok(t, err)

	err = recvSocket.SetIdentity(clientAddress)
	ifstest.Ok(t, err)

	err = recvSocket.Connect("tcp://" + communicator.GetOtherAddress(agentAddress))
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structures.Request{
		Id:          reqId,
		PayloadType: structures.FetchMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	_, err = senderSocket.SendBytes([]byte(agentAddress), zmq.SNDMORE)
	ifstest.Ok(t, err)

	_, err = senderSocket.SendBytes(data, 0)
	ifstest.Ok(t, err)

	frames, err := recvSocket.RecvMessageBytes(0)
	ifstest.Ok(t, err)

	ifstest.Compare(t, string(frames[0]), communicator.GetOtherAddress(agentAddress))

	data = frames[1]
	reply := &structures.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structures.FileMessageCode))
	ifstest.Compare(t, reply.Payload.GetFileMsg().File, fileData)

	foe.Stop()
	senderSocket.SetLinger(0)
	senderSocket.Close()
	recvSocket.SetLinger(0)
	recvSocket.Close()
	ifstest.RemoveTempFile(fileName)
}
