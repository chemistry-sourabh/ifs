package ifs

import (
	log "github.com/sirupsen/logrus"
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
		ConnId: req.ConnId,
		Id:     req.Id,
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

	}

	populateResponse(resp, data, err)

	a.Talker.SendResponse(resp)

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

	log.WithFields(log.Fields{
		"address": address,
		"port":    port,
	}).Info("Starting Agent")

	agent.Talker = talker
	agent.Watcher = watcher

	agent.Talker.Startup(address, port)

	err := agent.Watcher.Startup()

	if err != nil {
		log.Fatal("Watcher Failed")
	}

}
