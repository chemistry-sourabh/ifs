/*
Copyright 2018 Sourabh Bollapragada

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package ifs

import (
	"go.uber.org/zap"
	"sync"
)

type agent struct {
}

var (
	agentInstance *agent
	agentOnce     sync.Once
)

func Agent() *agent {
	agentOnce.Do(func() {
		agentInstance = &agent{}
	})

	return agentInstance
}

func populateResponse(resp *Packet, data Payload, err error) {

	if err == nil {
		resp.Data = data
	} else {
		resp.Data = &Error{
			Err: err,
		}
	}

	resp.Flags = 1
}

func (a *agent) ProcessRequest(req *Packet) {

	resp := &Packet{
		Id:     req.Id,
		ConnId: req.ConnId,
		Flags:  1,
	}

	var data Payload
	var err error

	switch req.Op {

	case AttrRequest:
		resp.Op = StatResponse
		data, err = AgentFileHandler().Attr(req)

	case ReadDirRequest:
		resp.Op = StatsResponse
		data, err = AgentFileHandler().ReadDir(req)
	case ReadDirAllRequest:
		resp.Op = StatsResponse
		data, err = AgentFileHandler().ReadDirAll(req)
	case FetchFileRequest:
		resp.Op = FileDataResponse
		data, err = AgentFileHandler().FetchFile(req)

	case ReadFileRequest:
		resp.Op = FileDataResponse
		data, err = AgentFileHandler().ReadFile(req)

	case WriteFileRequest:
		resp.Op = WriteResponse
		data, err = AgentFileHandler().WriteFile(req)

	case SetAttrRequest:
		resp.Op = ErrorResponse
		err = AgentFileHandler().SetAttr(req)

	case CreateRequest:
		resp.Op = ErrorResponse
		err = AgentFileHandler().CreateFile(req)

	case RemoveRequest:
		resp.Op = ErrorResponse
		err = AgentFileHandler().RemoveFile(req)

	case RenameRequest:
		resp.Op = ErrorResponse
		err = AgentFileHandler().RenameFile(req)

	case OpenRequest:
		resp.Op = ErrorResponse
		err = AgentFileHandler().OpenFile(req)
	case CloseRequest:
		resp.Op = ErrorResponse
		err = AgentFileHandler().CloseFile(req)
	}

	populateResponse(resp, data, err)

	AgentTalker().SendPacket(resp)

}

func StartAgent(address string, port uint16) {

	zap.L().Info("Starting Agent",
		zap.String("address", address),
		zap.Uint16("port", port),
	)

	AgentTalker().Startup(address, port)

}
