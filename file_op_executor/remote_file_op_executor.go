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
func (foe *RemoteFileOpExecutor) fetch(req *structures.FetchMessage) (*structures.FileMessage, error) {

	zap.L().Debug("Processing Fetch Message",
		zap.String("path", req.Path),
	)

	fileMsg := &structures.FileMessage{}
	filePath := req.Path

	data, err := ioutil.ReadFile(filePath)

	if err == nil {

		fileMsg.File = data

		//fileMsg.Compress()

		zap.L().Debug("Fetch Response",
			zap.String("path", filePath),
			zap.Int("size", len(data)),
			zap.Int("compressed_size", len(fileMsg.File)),
		)

		return fileMsg, err

	} else {
		zap.L().Error("Fetch Error",
			zap.String("path", filePath),
			zap.Error(err),
		)

	}

	return nil, err
}

func (foe *RemoteFileOpExecutor) open(req *structures.OpenMessage) error {

	zap.L().Debug("Processing Open Message",
		zap.String("path", req.Path),
		zap.Uint64("fd", req.Fd),
		zap.Int32("flags", req.Flags),
	)

	f, err := os.OpenFile(req.Path, int(req.Flags), 0666)

	if err != nil {

		zap.L().Error("Open Error",
			zap.String("path", req.Path),
			zap.Uint64("fd", req.Fd),
			zap.Int32("flags", req.Flags),
			zap.Error(err),
		)

		return err
	}

	foe.fp.Store(req.Fd, f)

	return nil
}

func (foe *RemoteFileOpExecutor) rename(req *structures.RenameMessage) error {
	zap.L().Debug("Processing Rename Message",
		zap.String("path", req.CurrentPath),
		zap.String("dest_path", req.NewPath),
	)

	err := os.Rename(req.CurrentPath, req.NewPath)

	if err != nil {
		zap.L().Error("Rename Error",
			zap.String("path", req.CurrentPath),
			zap.String("dest_path", req.NewPath),
			zap.Error(err),
		)
	}

	return err
}

func (foe *RemoteFileOpExecutor) create(req *structures.CreateMessage) error {
	filePath := path.Join(req.BaseDir, req.Name)

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

	foe.fp.Store(req.Fd, f)

	return nil
}

func (foe *RemoteFileOpExecutor) remove(req *structures.RemoveMessage) error {
	zap.L().Debug("Processing Remove Message",
		zap.String("path", req.Path),
	)

	err := os.Remove(req.Path)

	if err != nil {
		zap.L().Error("Remove Error",
			zap.String("path", req.Path),
			zap.Error(err),
		)
	}

	return err
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
						Payload: &structures.ReplyPayload_FileMsg{
							FileMsg: payload,
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
			 structures.RemoveMessageCode:
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
