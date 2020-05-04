/*
 * Copyright 2020 Sourabh Bollapragada
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
	"github.com/chemistry-sourabh/ifs/structure"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func TestAgentWebSocketReceiver_Comm(t *testing.T) {
	ifstest.SetupLogger()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(ifstest.GetOpenPort()))

	awr := NewAgentWebsocketReceiver()
	err := awr.Bind(agentAddress)
	ifstest.Ok(t, err)

	fm := &structure.FetchMessage{
		Path: "/tmp/test",
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_FetchMsg{
			FetchMsg: fm,
		},
	}

	dataMsg := &structure.DataMessage{
		Data: []byte("Hello World"),
	}

	replyPayload := &structure.ReplyPayload{
		Payload: &structure.ReplyPayload_DataMsg{
			DataMsg: dataMsg,
		},
	}

	time.Sleep(100 * time.Millisecond)

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 10000; i++ {
		request := &structure.Request{
			Id:          uint64(i),
			PayloadType: structure.FetchMessageCode,
			Payload:     requestPayload,
		}

		data, err := proto.Marshal(request)
		ifstest.Ok(t, err)

		err = conn.WriteMessage(websocket.BinaryMessage, data)
		ifstest.Ok(t, err)

		reqId, payloadType, recvPayload, err := awr.RecvRequest()
		ifstest.Ok(t, err)

		ifstest.Compare(t, reqId, request.Id)
		ifstest.Compare(t, payloadType, uint32(structure.FetchMessageCode))

		recvFm := recvPayload.GetFetchMsg()
		ifstest.Compare(t, fm.Path, recvFm.Path)

		err = awr.SendReply(reqId, structure.DataMessageCode, replyPayload)
		ifstest.Ok(t, err)

		messageType, data, err := conn.ReadMessage()
		ifstest.Ok(t, err)
		ifstest.Compare(t, messageType, websocket.BinaryMessage)

		reply := &structure.Reply{}
		err = proto.Unmarshal(data, reply)
		ifstest.Ok(t, err)

		ifstest.Compare(t, reply.Id, reqId)
		ifstest.Compare(t, reply.PayloadType, uint32(structure.DataMessageCode))
		ifstest.Compare(t, string(reply.Payload.GetDataMsg().GetData()), "Hello World")
	}

	conn.Close()
	awr.Unbind()
}
