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
	"gopkg.in/zeromq/goczmq.v4"
	"testing"
	"time"
)

func TestAgentTcpReceiver_Comm(t *testing.T) {
	clientAddress := "127.0.0.1:5000"
	agentAddress := "127.0.0.1:5001"

	atr := NewAgentTcpReceiver()
	err := atr.Bind(agentAddress)
	ifstest.Ok(t, err)

	fm := &structures.FetchMessage{
		Path: "/tmp/test",
	}

	payload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_FetchMsg{
			FetchMsg: fm,
		},
	}

	sock := goczmq.NewSock(goczmq.Router)
	sock.SetIdentity(clientAddress)
	err = sock.Connect("tcp://" + agentAddress)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 10000; i++ {
		request := &structures.Request{
			Id:          uint64(i),
			PayloadType: structures.FetchMessageCode,
			Payload:     payload,
		}

		data, err := proto.Marshal(request)
		ifstest.Ok(t, err)

		err = sock.SendMessage([][]byte{[]byte(agentAddress), data})
		ifstest.Ok(t, err)

		reqId, payloadType, address, recvPayload, err := atr.RecvRequest()
		ifstest.Ok(t, err)

		ifstest.Compare(t, request.Id, reqId)
		ifstest.Compare(t, request.PayloadType, payloadType)
		ifstest.Compare(t, clientAddress, address)

		recvFm := recvPayload.GetFetchMsg()
		ifstest.Compare(t, fm.Path, recvFm.Path)


	}

	atr.Unbind()
	sock.Destroy()
}
