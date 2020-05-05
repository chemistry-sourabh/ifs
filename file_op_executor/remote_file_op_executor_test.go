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
	"github.com/chemistry-sourabh/ifs/structure"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
	"io/ioutil"
	"math/rand"
	"net/url"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

func TestRemoteFileOpExecutor_Fetch(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)
	fileData := ifstest.WriteDummyData(fileName, 1000)

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	fm := &structure.FetchMessage{
		Path: path.Join("/tmp", fileName),
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_FetchMsg{
			FetchMsg: fm,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.FetchMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.DataMessageCode))
	ifstest.Compare(t, reply.Payload.GetDataMsg().Data, fileData)

	conn.Close()
	foe.Stop()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Open(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	om := &structure.OpenMessage{
		Fd:    1,
		Path:  path.Join("/tmp", fileName),
		Flags: uint32(os.O_RDONLY),
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_OpenMsg{
			OpenMsg: om,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.OpenMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.OkMessageCode))

	val, _ := foe.fp.Load(uint64(1))
	val.(*os.File).Close()

	conn.Close()
	foe.Stop()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Create(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	cm := &structure.CreateMessage{
		Fd:      1,
		BaseDir: "/tmp",
		Name:    fileName,
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_CreateMsg{
			CreateMsg: cm,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.CreateMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.OkMessageCode))

	val, _ := foe.fp.Load(uint64(1))
	val.(*os.File).Close()

	conn.Close()
	foe.Stop()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Rename(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"
	newFileName := "file2"

	ifstest.CreateTempFile(fileName)
	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	rm := &structure.RenameMessage{
		CurrentPath: path.Join("/tmp", fileName),
		NewPath:     path.Join("/tmp", newFileName),
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_RenameMsg{
			RenameMsg: rm,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.RenameMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.OkMessageCode))

	_, err = os.Stat(path.Join("/tmp", fileName))
	ifstest.Err(t, err)

	_, err = os.Stat(path.Join("/tmp", newFileName))
	ifstest.Ok(t, err)

	conn.Close()
	foe.Stop()
	ifstest.RemoveTempFile(newFileName)
}

func TestRemoteFileOpExecutor_Remove(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	rm := &structure.RemoveMessage{
		Path: path.Join("/tmp", fileName),
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_RemoveMsg{
			RemoveMsg: rm,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.RemoveMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.OkMessageCode))

	_, err = os.Stat(path.Join("/tmp", fileName))
	ifstest.Err(t, err)

	conn.Close()
	foe.Stop()

}

func TestRemoteFileOpExecutor_Close(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	om := &structure.OpenMessage{
		Fd:    1,
		Path:  path.Join("/tmp", fileName),
		Flags: uint32(os.O_RDONLY),
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_OpenMsg{
			OpenMsg: om,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.OpenMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	// Send Close Request
	cm := &structure.CloseMessage{
		Fd: 1,
	}

	requestPayload = &structure.RequestPayload{
		Payload: &structure.RequestPayload_CloseMsg{
			CloseMsg: cm,
		},
	}

	reqId = rand.Uint64()

	request = &structure.Request{
		Id:          reqId,
		PayloadType: structure.CloseMessageCode,
		Payload:     requestPayload,
	}

	data, err = proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err = conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.OkMessageCode))

	_, ok := foe.fp.Load(uint64(1))
	ifstest.Compare(t, ok, false)

	conn.Close()
	foe.Stop()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Truncate(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)
	ifstest.WriteDummyData(fileName, 1000)

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	tm := &structure.TruncateMessage{
		Path: path.Join("/tmp", fileName),
		Size: uint64(100),
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_TruncateMsg{
			TruncateMsg: tm,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.TruncateMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.OkMessageCode))

	stat, err := os.Stat(path.Join("/tmp", fileName))
	ifstest.Ok(t, err)

	ifstest.Compare(t, stat.Size(), int64(100))

	conn.Close()
	foe.Stop()

	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Flush(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	cm := &structure.CreateMessage{
		Fd:      1,
		BaseDir: "/tmp",
		Name:    fileName,
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_CreateMsg{
			CreateMsg: cm,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.CreateMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	// Send Flush Request
	fm := &structure.FlushMessage{
		Fd: 1,
	}

	requestPayload = &structure.RequestPayload{
		Payload: &structure.RequestPayload_FlushMsg{
			FlushMsg: fm,
		},
	}

	reqId = rand.Uint64()

	request = &structure.Request{
		Id:          reqId,
		PayloadType: structure.FlushMessageCode,
		Payload:     requestPayload,
	}

	data, err = proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err = conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.OkMessageCode))

	val, _ := foe.fp.Load(uint64(1))
	val.(*os.File).Close()

	conn.Close()
	foe.Stop()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Read(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)
	fileData := ifstest.WriteDummyData(fileName, 1000)

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	om := &structure.OpenMessage{
		Fd:    1,
		Path:  path.Join("/tmp", fileName),
		Flags: uint32(os.O_RDONLY),
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_OpenMsg{
			OpenMsg: om,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.OpenMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	// Send Read Message
	rm := &structure.ReadMessage{
		Fd:     1,
		Offset: 0,
		Size:   1000,
	}

	requestPayload = &structure.RequestPayload{
		Payload: &structure.RequestPayload_ReadMsg{
			ReadMsg: rm,
		},
	}

	reqId = rand.Uint64()

	request = &structure.Request{
		Id:          reqId,
		PayloadType: structure.ReadMessageCode,
		Payload:     requestPayload,
	}

	data, err = proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err = conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.DataMessageCode))
	ifstest.Compare(t, reply.Payload.GetDataMsg().GetData(), fileData)

	val, _ := foe.fp.Load(uint64(1))
	val.(*os.File).Close()

	conn.Close()
	foe.Stop()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Write(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	om := &structure.OpenMessage{
		Fd:    1,
		Path:  path.Join("/tmp", fileName),
		Flags: uint32(os.O_WRONLY),
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_OpenMsg{
			OpenMsg: om,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.OpenMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	// Send Write Message

	data = make([]byte, 1000)
	_, err = rand.Read(data)
	ifstest.Ok(t, err)

	wm := &structure.WriteMessage{
		Fd:     1,
		Offset: 0,
		Data:   data,
	}

	requestPayload = &structure.RequestPayload{
		Payload: &structure.RequestPayload_WriteMsg{
			WriteMsg: wm,
		},
	}

	reqId = rand.Uint64()

	request = &structure.Request{
		Id:          reqId,
		PayloadType: structure.WriteMessageCode,
		Payload:     requestPayload,
	}

	data, err = proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err = conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.WriteOkMessageCode))
	ifstest.Compare(t, reply.Payload.GetWriteOkMsg().GetSize(), uint64(1000))

	val, _ := foe.fp.Load(uint64(1))
	val.(*os.File).Close()

	conn.Close()
	foe.Stop()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Attr(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)
	ifstest.WriteDummyData(fileName, 1000)

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	am := &structure.AttrMessage{
		Path: path.Join("/tmp", fileName),
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_AttrMsg{
			AttrMsg: am,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.AttrMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.FileInfoMessageCode))

	fi := reply.GetPayload().GetFileInfoMsg()

	stat, err := os.Stat(path.Join("/tmp", fileName))
	ifstest.Ok(t, err)

	ifstest.Compare(t, fi.GetSize(), uint64(stat.Size()))
	ifstest.Compare(t, fi.GetMode(), uint32(stat.Mode()))
	ifstest.Compare(t, fi.GetMtime(), uint64(stat.ModTime().UnixNano()))

	conn.Close()
	foe.Stop()

	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_ReadDir(t *testing.T) {
	ifstest.SetupLogger()

	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))

	os.MkdirAll("/tmp/test", 0755)
	ifstest.CreateTempFile("test/file1")
	ifstest.CreateTempFile("test/file2")
	ifstest.WriteDummyData("test/file1", 1000)
	ifstest.WriteDummyData("test/file2", 2000)

	atr := communicator.NewAgentWebSocketReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	rdm := &structure.ReadDirMessage{
		Path: "/tmp/test",
	}

	requestPayload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_ReadDirMsg{
			ReadDirMsg: rdm,
		},
	}

	u := url.URL{Scheme: "ws", Host: agentAddress, Path: "/"}
	websocket.DefaultDialer.EnableCompression = true

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structure.Request{
		Id:          reqId,
		PayloadType: structure.ReadDirMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	err = conn.WriteMessage(websocket.BinaryMessage, data)
	ifstest.Ok(t, err)

	messageType, data, err := conn.ReadMessage()
	ifstest.Ok(t, err)

	ifstest.Compare(t, messageType, websocket.BinaryMessage)

	reply := &structure.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structure.FileInfosMessageCode))

	fi := reply.GetPayload().GetFileInfosMsg().GetFileInfos()

	stats, err := ioutil.ReadDir("/tmp/test")
	ifstest.Ok(t, err)

	for i := range stats {
		ifstest.Compare(t, fi[i].GetSize(), uint64(stats[i].Size()))
		ifstest.Compare(t, fi[i].GetMode(), uint32(stats[i].Mode()))
		ifstest.Compare(t, fi[i].GetMtime(), uint64(stats[i].ModTime().UnixNano()))
		ifstest.Compare(t, fi[i].IsDir, false)
	}

	conn.Close()
	foe.Stop()

	ifstest.RemoveTempFile("test/file1")
	ifstest.RemoveTempFile("test/file2")
	os.Remove("/tmp/test")
}
