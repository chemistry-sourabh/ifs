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
	"github.com/chemistry-sourabh/ifs/ifstest"
	"github.com/chemistry-sourabh/ifs/structures"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"strconv"
	"testing"
	"time"
)

func TestAgentZmqReceiver_Comm(t *testing.T) {
	//time.Sleep(ifstest.TEST_WAIT)

	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(ifstest.GetOpenPort()))

	azr := NewAgentZmqReceiver()
	err := azr.Bind(agentAddress)
	ifstest.Ok(t, err)

	fm := &structures.FetchMessage{
		Path: "/tmp/test",
	}

	requestPayload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_FetchMsg{
			FetchMsg: fm,
		},
	}

	fileMsg := &structures.FileMessage{
		File: []byte("Hello World"),
	}

	replyPayload := &structures.ReplyPayload{
		Payload: &structures.ReplyPayload_FileMsg{
			FileMsg: fileMsg,
		},
	}

	ctx, err := zmq.NewContext()
	ifstest.Ok(t, err)

	senderSocket, err := ctx.NewSocket(zmq.ROUTER)
	ifstest.Ok(t, err)

	err = senderSocket.SetIdentity(clientAddress)
	ifstest.Ok(t, err)

	err = senderSocket.Connect("tcp://" + agentAddress)
	ifstest.Ok(t, err)

	recvSocket, err := ctx.NewSocket(zmq.ROUTER)
	ifstest.Ok(t, err)

	err = recvSocket.SetIdentity(clientAddress)
	ifstest.Ok(t, err)

	err = recvSocket.Connect("tcp://" + GetOtherAddress(agentAddress))
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 10000; i++ {
		request := &structures.Request{
			Id:          uint64(i),
			PayloadType: structures.FetchMessageCode,
			Payload:     requestPayload,
		}

		data, err := proto.Marshal(request)
		ifstest.Ok(t, err)

		_, err = senderSocket.SendBytes([]byte(agentAddress), zmq.SNDMORE)
		ifstest.Ok(t, err)

		_, err = senderSocket.SendBytes(data, 0)
		ifstest.Ok(t, err)

		reqId, payloadType, recvPayload, err := azr.RecvRequest()
		ifstest.Ok(t, err)

		ifstest.Compare(t, reqId, request.Id)
		ifstest.Compare(t, payloadType, uint32(structures.FetchMessageCode))

		recvFm := recvPayload.GetFetchMsg()
		ifstest.Compare(t, fm.Path, recvFm.Path)

		err = azr.SendReply(reqId, structures.FileMessageCode, replyPayload)
		ifstest.Ok(t, err)

		frames, err := recvSocket.RecvMessageBytes(0)
		ifstest.Ok(t, err)

		ifstest.Compare(t, string(frames[0]), GetOtherAddress(agentAddress))

		data = frames[1]
		reply := &structures.Reply{}
		err = proto.Unmarshal(data, reply)
		ifstest.Ok(t, err)

		ifstest.Compare(t, reply.Id, reqId)
		ifstest.Compare(t, reply.PayloadType, uint32(structures.FileMessageCode))
		ifstest.Compare(t, string(reply.Payload.GetFileMsg().File), "Hello World")
	}

	senderSocket.SetLinger(0)
	senderSocket.Close()
	recvSocket.SetLinger(0)
	recvSocket.Close()
	azr.Unbind()
	ctx.Term()
}
