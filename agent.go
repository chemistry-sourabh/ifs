package ifs

import (
	"go.uber.org/zap"
	"sync"
)

type agent struct {
}

var (
	agentInstance *agent
	agentOnce sync.Once
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

	case WatchDirRequest:
		resp.Op = ErrorResponse
		err = Watcher().WatchPaths(req)

	}

	populateResponse(resp, data, err)

	AgentTalker().SendPacket(resp)

}

func StartAgent(address string, port uint16) {


	zap.L().Info("Starting Agent",
		zap.String("address", address),
		zap.Uint16("port", port),
	)


	err := Watcher().Startup()

	if err != nil {
		zap.L().Fatal("Watcher Failed",
			zap.Error(err),
		)
	}

	AgentTalker().Startup(address, port)

}
