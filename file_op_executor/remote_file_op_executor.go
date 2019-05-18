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
	"github.com/chemistry-sourabh/ifs/structures"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

type RemoteFileOpExecutor struct {
	Receiver communicator.Receiver
	fp       sync.Map
}

// TODO Add a type attribute for receiver
func NewRemoteFileOpExecutor() *RemoteFileOpExecutor {
	return &RemoteFileOpExecutor{
		fp: sync.Map{},
	}
}

func (foe *RemoteFileOpExecutor) createErrMsg(err error) *structures.ReplyPayload {
	return &structures.ReplyPayload{
		Payload: &structures.ReplyPayload_ErrMsg{
			ErrMsg: &structures.ErrMessage{
				Error: err.Error(),
			},
		},
	}
}

// TODO Check Compression
func (foe *RemoteFileOpExecutor) fetch(req *structures.FetchMessage) (*structures.DataMessage, error) {

	zap.L().Debug("Processing Fetch Message",
		zap.String("path", req.GetPath()),
	)

	dataMsg := &structures.DataMessage{}
	data, err := ioutil.ReadFile(req.GetPath())

	if err == nil {

		dataMsg.Data = data

		//dataMsg.Compress()

		zap.L().Debug("Fetch Response",
			zap.String("path", req.GetPath()),
			zap.Int("size", len(data)),
			zap.Int("compressed_size", len(dataMsg.GetData())),
		)

		return dataMsg, err

	} else {
		zap.L().Error("Fetch Error",
			zap.String("path", req.GetPath()),
			zap.Error(err),
		)

	}

	return nil, err
}

func (foe *RemoteFileOpExecutor) open(req *structures.OpenMessage) error {

	zap.L().Debug("Processing Open Message",
		zap.String("path", req.GetPath()),
		zap.Uint64("fd", req.GetFd()),
		zap.Uint32("flags", req.GetFlags()),
	)

	f, err := os.OpenFile(req.GetPath(), int(req.GetFlags()), 0666)

	if err != nil {

		zap.L().Error("Open Error",
			zap.String("path", req.GetPath()),
			zap.Uint64("fd", req.GetFd()),
			zap.Uint32("flags", req.GetFlags()),
			zap.Error(err),
		)

		return err
	}

	foe.fp.Store(req.GetFd(), f)

	return nil
}

func (foe *RemoteFileOpExecutor) rename(req *structures.RenameMessage) error {
	zap.L().Debug("Processing Rename Message",
		zap.String("path", req.GetCurrentPath()),
		zap.String("dest_path", req.GetNewPath()),
	)

	err := os.Rename(req.GetCurrentPath(), req.GetNewPath())

	if err != nil {
		zap.L().Error("Rename Error",
			zap.String("path", req.GetCurrentPath()),
			zap.String("dest_path", req.GetNewPath()),
			zap.Error(err),
		)
	}

	return err
}

func (foe *RemoteFileOpExecutor) create(req *structures.CreateMessage) error {
	filePath := path.Join(req.GetBaseDir(), req.GetName())

	zap.L().Debug("Processing Create Message",
		zap.String("path", filePath),
	)

	f, err := os.Create(filePath)
	if err != nil {

		zap.L().Error("Create Error",
			zap.String("path", filePath),
			zap.Error(err),
		)
		return err
	}

	foe.fp.Store(req.GetFd(), f)

	return nil
}

func (foe *RemoteFileOpExecutor) remove(req *structures.RemoveMessage) error {
	zap.L().Debug("Processing Remove Message",
		zap.String("path", req.GetPath()),
	)

	err := os.Remove(req.GetPath())

	if err != nil {
		zap.L().Error("Remove Error",
			zap.String("path", req.GetPath()),
			zap.Error(err),
		)
	}

	return err
}

func (foe *RemoteFileOpExecutor) close(req *structures.CloseMessage) error {
	zap.L().Debug("Processing Close Message",
		zap.Uint64("fd", req.GetFd()),
	)

	val, ok := foe.fp.Load(req.GetFd())

	if !ok {

		zap.L().Error("Close Error",
			zap.Uint64("fd", req.GetFd()),
			zap.Error(os.ErrInvalid),
		)

		return os.ErrInvalid
	}

	f := val.(*os.File)
	err := f.Close()

	if err != nil {

		zap.L().Error("Close Error",
			zap.Uint64("fd", req.GetFd()),
			zap.Error(err),
		)

		return err
	}

	foe.fp.Delete(req.GetFd())

	return nil
}

func (foe *RemoteFileOpExecutor) flush(req *structures.FlushMessage) error {
	zap.L().Debug("Processing Flush Message",
		zap.Uint64("fd", req.GetFd()),
	)

	val, ok := foe.fp.Load(req.GetFd())

	if !ok {

		zap.L().Error("Flush Error",
			zap.Uint64("fd", req.GetFd()),
			zap.Error(os.ErrInvalid),
		)

		return os.ErrInvalid
	}

	f := val.(*os.File)
	err := f.Sync()

	if err != nil {

		zap.L().Error("Flush Error",
			zap.Uint64("fd", req.GetFd()),
			zap.Error(err),
		)

		return err
	}

	return nil
}

func (foe *RemoteFileOpExecutor) truncate(req *structures.TruncateMessage) error {
	zap.L().Debug("Processing Truncate Message",
		zap.String("path", req.GetPath()),
		zap.Uint64("size", req.GetSize()),
	)

	err := os.Truncate(req.Path, int64(req.GetSize()))

	if err != nil {

		zap.L().Error("Truncate Error",
			zap.String("path", req.GetPath()),
			zap.Uint64("size", req.GetSize()),
			zap.Error(err),
		)

		return err
	}

	return nil
}

func (foe *RemoteFileOpExecutor) Process() {

	for {
		id, payloadType, req, err := foe.Receiver.RecvRequest()

		if err != nil {
			break
		}

		zap.L().Debug("Processing Request",
			zap.Uint64("id", id),
			zap.Uint32("type", payloadType),
		)

		switch payloadType {
		case structures.FetchMessageCode:
			go func() {
				payload, err := foe.fetch(req.GetFetchMsg())

				var reply *structures.ReplyPayload
				var replyType uint32 = structures.ErrMessageCode
				if err != nil {
					reply = foe.createErrMsg(err)
				} else {

					reply = &structures.ReplyPayload{
						Payload: &structures.ReplyPayload_DataMsg{
							DataMsg: payload,
						},
					}

					replyType = structures.FileMessageCode

				}

				err = foe.Receiver.SendReply(id, replyType, reply)

				if err != nil {
					zap.L().Warn("Failed to Send Reply",
						zap.Uint64("id", id),
						zap.Uint32("payloadType", payloadType),
						zap.Error(err),
					)
				}
			}()

		case structures.OpenMessageCode, structures.RenameMessageCode, structures.CreateMessageCode,
			 structures.RemoveMessageCode, structures.CloseMessageCode, structures.TruncateMessageCode,
			 structures.FlushMessageCode:
			go func() {
				var err error
				switch payloadType {
				case structures.OpenMessageCode:
					err = foe.open(req.GetOpenMsg())
				case structures.RenameMessageCode:
					err = foe.rename(req.GetRenameMsg())
				case structures.CreateMessageCode:
					err = foe.create(req.GetCreateMsg())
				case structures.RemoveMessageCode:
					err = foe.remove(req.GetRemoveMsg())
				case structures.CloseMessageCode:
					err = foe.close(req.GetCloseMsg())
				case structures.TruncateMessageCode:
					err = foe.truncate(req.GetTruncateMsg())
				case structures.FlushMessageCode:
					err = foe.flush(req.GetFlushMsg())
				}

				var reply *structures.ReplyPayload
				var replyType uint32 = structures.ErrMessageCode
				if err != nil {
					reply = foe.createErrMsg(err)
				} else {

					reply = &structures.ReplyPayload{}
					replyType = structures.OkMessageCode

				}

				err = foe.Receiver.SendReply(id, replyType, reply)

				if err != nil {
					zap.L().Warn("Failed to Send Reply",
						zap.Uint64("id", id),
						zap.Uint32("payloadType", payloadType),
						zap.Error(err),
					)
				}
			}()
		}
	}

}

func (foe *RemoteFileOpExecutor) Stop() {
	foe.Receiver.Unbind()
}

func (foe *RemoteFileOpExecutor) Run(address string) {
	err := foe.Receiver.Bind(address)

	if err != nil {
		zap.L().Fatal("Failed to Bind Receiver",
			zap.String("address", address),
		)
	}

	foe.Process()
}
