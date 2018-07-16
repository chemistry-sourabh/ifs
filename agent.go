package ifs

import (
	"go.uber.org/zap"
)

type Agent struct {
	Talker      *AgentTalker
	FileHandler *AgentFileHandler
	Watcher     *Watcher
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

func (a *Agent) ProcessRequest(req *Packet) {

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
		data, err = a.FileHandler.Attr(req)

	case ReadDirRequest:
		resp.Op = StatsResponse
		data, err = a.FileHandler.ReadDir(req)
	case ReadDirAllRequest:
		resp.Op = StatsResponse
		data, err = a.FileHandler.ReadDirAll(req)
	case FetchFileRequest:
		resp.Op = FileDataResponse
		data, err = a.FileHandler.FetchFile(req)

	case ReadFileRequest:
		resp.Op = FileDataResponse
		data, err = a.FileHandler.ReadFile(req)

	case WriteFileRequest:
		resp.Op = WriteResponse
		data, err = a.FileHandler.WriteFile(req)

	case SetAttrRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.SetAttr(req)

	case CreateRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.CreateFile(req)

	case RemoveRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.RemoveFile(req)

	case RenameRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.RenameFile(req)

	case OpenRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.OpenFile(req)
	case CloseRequest:
		resp.Op = ErrorResponse
		err = a.FileHandler.CloseFile(req)

	case WatchDirRequest:
		resp.Op = ErrorResponse
		err = a.Watcher.WatchPaths(req)

	}

	populateResponse(resp, data, err)

	a.Talker.SendPacket(resp)

}

func StartAgent(address string, port uint16) {
	agent := &Agent{
		FileHandler: NewAgentFileHandler(),
	}

	talker := &AgentTalker{
		Agent: agent,
		Pool:  NewAgentConnectionPool(),
	}

	watcher := &Watcher{
		Agent: agent,
	}

	zap.L().Info("Starting Agent",
		zap.String("address", address),
		zap.Uint16("port", port),
	)

	agent.Talker = talker
	agent.Watcher = watcher

	err := agent.Watcher.Startup()

	if err != nil {
		zap.L().Fatal("Watcher Failed",
			zap.Error(err),
		)
	}

	agent.Talker.Startup(address, port)

}
