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
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

func TestRemoteFileOpExecutor_Fetch(t *testing.T) {
	ifstest.SetupLogger()

	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
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

	_, err = senderSocket.SendMessage([][]byte{[]byte(agentAddress), data})
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
	ifstest.Compare(t, reply.Payload.GetDataMsg().Data, fileData)

	recvSocket.SetLinger(0)
	recvSocket.Close()
	senderSocket.SetLinger(0)
	senderSocket.Close()
	foe.Stop()
	ctx.Term()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Open(t *testing.T) {
	ifstest.SetupLogger()

	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)

	atr := communicator.NewAgentZmqReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	om := &structures.OpenMessage{
		Fd: 1,
		Path: path.Join("/tmp", fileName),
		Flags: uint32(os.O_RDONLY),
	}

	requestPayload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_OpenMsg{
			OpenMsg: om,
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

	err = recvSocket.Connect("tcp://" + communicator.GetOtherAddress(agentAddress))
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structures.Request{
		Id:          reqId,
		PayloadType: structures.OpenMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	_, err = senderSocket.SendMessage([][]byte{[]byte(agentAddress), data})
	ifstest.Ok(t, err)

	frames, err := recvSocket.RecvMessageBytes(0)
	ifstest.Ok(t, err)

	ifstest.Compare(t, string(frames[0]), communicator.GetOtherAddress(agentAddress))

	data = frames[1]
	reply := &structures.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structures.OkMessageCode))

	val, _ := foe.fp.Load(uint64(1))
	val.(*os.File).Close()

	recvSocket.SetLinger(0)
	recvSocket.Close()
	senderSocket.SetLinger(0)
	senderSocket.Close()
	foe.Stop()
	ctx.Term()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Create(t *testing.T) {
	ifstest.SetupLogger()

	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	atr := communicator.NewAgentZmqReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	cm := &structures.CreateMessage{
		Fd: 1,
		BaseDir:"/tmp",
		Name: fileName,
	}

	requestPayload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_CreateMsg{
			CreateMsg: cm,
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

	err = recvSocket.Connect("tcp://" + communicator.GetOtherAddress(agentAddress))
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structures.Request{
		Id:          reqId,
		PayloadType: structures.CreateMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	_, err = senderSocket.SendMessage([][]byte{[]byte(agentAddress), data})
	ifstest.Ok(t, err)

	frames, err := recvSocket.RecvMessageBytes(0)
	ifstest.Ok(t, err)

	ifstest.Compare(t, string(frames[0]), communicator.GetOtherAddress(agentAddress))

	data = frames[1]
	reply := &structures.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structures.OkMessageCode))

	val, _ := foe.fp.Load(uint64(1))
	val.(*os.File).Close()

	recvSocket.SetLinger(0)
	recvSocket.Close()
	senderSocket.SetLinger(0)
	senderSocket.Close()
	foe.Stop()
	ctx.Term()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Rename(t *testing.T) {
	ifstest.SetupLogger()

	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"
	newFileName := "file2"

	ifstest.CreateTempFile(fileName)
	atr := communicator.NewAgentZmqReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	rm := &structures.RenameMessage{
		CurrentPath: path.Join("/tmp", fileName),
		NewPath: path.Join("/tmp", newFileName),
	}

	requestPayload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_RenameMsg{
			RenameMsg: rm,
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

	err = recvSocket.Connect("tcp://" + communicator.GetOtherAddress(agentAddress))
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structures.Request{
		Id:          reqId,
		PayloadType: structures.RenameMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	_, err = senderSocket.SendMessage([][]byte{[]byte(agentAddress), data})
	ifstest.Ok(t, err)

	frames, err := recvSocket.RecvMessageBytes(0)
	ifstest.Ok(t, err)

	ifstest.Compare(t, string(frames[0]), communicator.GetOtherAddress(agentAddress))

	data = frames[1]
	reply := &structures.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structures.OkMessageCode))

	_, err = os.Stat(path.Join("/tmp", fileName))
	ifstest.Err(t, err)

	_, err = os.Stat(path.Join("/tmp", newFileName))
	ifstest.Ok(t, err)

	recvSocket.SetLinger(0)
	recvSocket.Close()
	senderSocket.SetLinger(0)
	senderSocket.Close()
	foe.Stop()
	ctx.Term()
	ifstest.RemoveTempFile(newFileName)
}

func TestRemoteFileOpExecutor_Remove(t *testing.T) {
	ifstest.SetupLogger()

	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)

	atr := communicator.NewAgentZmqReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	rm := &structures.RemoveMessage{
		Path: path.Join("/tmp", fileName),
	}

	requestPayload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_RemoveMsg{
			RemoveMsg: rm,
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

	err = recvSocket.Connect("tcp://" + communicator.GetOtherAddress(agentAddress))
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structures.Request{
		Id:          reqId,
		PayloadType: structures.RemoveMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	_, err = senderSocket.SendMessage([][]byte{[]byte(agentAddress), data})
	ifstest.Ok(t, err)

	frames, err := recvSocket.RecvMessageBytes(0)
	ifstest.Ok(t, err)

	ifstest.Compare(t, string(frames[0]), communicator.GetOtherAddress(agentAddress))

	data = frames[1]
	reply := &structures.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structures.OkMessageCode))

	_, err = os.Stat(path.Join("/tmp", fileName))
	ifstest.Err(t, err)

	recvSocket.SetLinger(0)
	recvSocket.Close()
	senderSocket.SetLinger(0)
	senderSocket.Close()
	foe.Stop()
	ctx.Term()

}

func TestRemoteFileOpExecutor_Close(t *testing.T) {
	ifstest.SetupLogger()

	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)

	atr := communicator.NewAgentZmqReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	om := &structures.OpenMessage{
		Fd: 1,
		Path: path.Join("/tmp", fileName),
		Flags: uint32(os.O_RDONLY),
	}

	requestPayload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_OpenMsg{
			OpenMsg: om,
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

	err = recvSocket.Connect("tcp://" + communicator.GetOtherAddress(agentAddress))
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structures.Request{
		Id:          reqId,
		PayloadType: structures.OpenMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	_, err = senderSocket.SendMessage([][]byte{[]byte(agentAddress), data})
	ifstest.Ok(t, err)

	_, err = recvSocket.RecvMessageBytes(0)
	ifstest.Ok(t, err)

	// Send Close Request
	cm := &structures.CloseMessage{
		Fd: 1,
	}

	requestPayload = &structures.RequestPayload{
		Payload: &structures.RequestPayload_CloseMsg{
			CloseMsg: cm,
		},
	}

	reqId = rand.Uint64()

	request = &structures.Request{
		Id:          reqId,
		PayloadType: structures.CloseMessageCode,
		Payload:     requestPayload,
	}

	data, err = proto.Marshal(request)
	ifstest.Ok(t, err)

	_, err = senderSocket.SendMessage([][]byte{[]byte(agentAddress), data})
	ifstest.Ok(t, err)

	frames, err := recvSocket.RecvMessageBytes(0)
	ifstest.Ok(t, err)

	ifstest.Compare(t, string(frames[0]), communicator.GetOtherAddress(agentAddress))

	data = frames[1]
	reply := &structures.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structures.OkMessageCode))

	_, ok := foe.fp.Load(uint64(1))
	ifstest.Compare(t, ok, false)

	recvSocket.SetLinger(0)
	recvSocket.Close()
	senderSocket.SetLinger(0)
	senderSocket.Close()
	foe.Stop()
	ctx.Term()
	ifstest.RemoveTempFile(fileName)
}

func TestRemoteFileOpExecutor_Truncate(t *testing.T) {
	ifstest.SetupLogger()

	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	fileName := "file1"

	ifstest.CreateTempFile(fileName)
	ifstest.WriteDummyData(fileName, 1000)

	atr := communicator.NewAgentZmqReceiver()
	foe := NewRemoteFileOpExecutor()
	foe.Receiver = atr

	go foe.Run(agentAddress)

	time.Sleep(100 * time.Millisecond)

	tm := &structures.TruncateMessage{
		Path: path.Join("/tmp", fileName),
		Size: uint64(100),
	}

	requestPayload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_TruncateMsg{
			TruncateMsg: tm,
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

	err = recvSocket.Connect("tcp://" + communicator.GetOtherAddress(agentAddress))
	ifstest.Ok(t, err)

	time.Sleep(100 * time.Millisecond)

	reqId := rand.Uint64()

	request := &structures.Request{
		Id:          reqId,
		PayloadType: structures.TruncateMessageCode,
		Payload:     requestPayload,
	}

	data, err := proto.Marshal(request)
	ifstest.Ok(t, err)

	_, err = senderSocket.SendMessage([][]byte{[]byte(agentAddress), data})
	ifstest.Ok(t, err)

	frames, err := recvSocket.RecvMessageBytes(0)
	ifstest.Ok(t, err)

	ifstest.Compare(t, string(frames[0]), communicator.GetOtherAddress(agentAddress))

	data = frames[1]
	reply := &structures.Reply{}
	err = proto.Unmarshal(data, reply)
	ifstest.Ok(t, err)

	ifstest.Compare(t, reply.Id, reqId)
	ifstest.Compare(t, reply.PayloadType, uint32(structures.OkMessageCode))

	stat, err := os.Stat(path.Join("/tmp", fileName))
	ifstest.Ok(t, err)

	ifstest.Compare(t, stat.Size(), int64(100))

	recvSocket.SetLinger(0)
	recvSocket.Close()
	senderSocket.SetLinger(0)
	senderSocket.Close()
	foe.Stop()
	ctx.Term()

	ifstest.RemoveTempFile(fileName)
}
