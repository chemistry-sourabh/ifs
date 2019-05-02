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
)

type RemoteFileOpExecutor struct {
	Receiver communicator.Receiver
	fp       map[uint64]*os.File
}

// TODO Add a type attribute for receiver
func NewRemoteFileOpExecutor() *RemoteFileOpExecutor {
	return &RemoteFileOpExecutor{
		fp: make(map[uint64]*os.File),
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
		zap.L().Warn("Fetch Error",
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

		zap.L().Debug("Open Error",
			zap.String("path", req.Path),
			zap.Uint64("fd", req.Fd),
			zap.Int32("flags", req.Flags),
			zap.Error(err),
		)

		return err
	}

	foe.fp[req.Fd] = f

	return nil
}

func (foe *RemoteFileOpExecutor) rename(req *structures.RenameMessage) error {
	return nil
}

// TODO go routines for each task
func (foe *RemoteFileOpExecutor) Process() {

	for {
		id, payloadType, address, req, err := foe.Receiver.RecvRequest()

		if err != nil {
			zap.L().Fatal("Couldn't Receive Message",
				zap.Error(err),
			)
		}

		zap.L().Debug("Processing Request",
			zap.Uint64("id", id),
			zap.Uint32("type", payloadType),
			zap.String("address", address),
		)

		switch payloadType {
		case structures.FetchMessageCode:
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

			err = foe.Receiver.SendReply(id, replyType, address, reply)

			if err != nil {
				zap.L().Warn("Failed to Send Reply",
					zap.Uint64("id", id),
					zap.Uint32("payloadType", payloadType),
					zap.String("address", address),
					zap.Error(err),
				)
			}

		case structures.OpenMessageCode:
			err := foe.open(req.GetOpenMsg())

			var reply *structures.ReplyPayload
			var replyType uint32 = structures.ErrMessageCode
			if err != nil {
				reply = foe.createErrMsg(err)
			} else {

				reply = &structures.ReplyPayload{}
				replyType = structures.OkMessageCode

			}

			err = foe.Receiver.SendReply(id, replyType, address, reply)

			if err != nil {
				zap.L().Warn("Failed to Send Reply",
					zap.Uint64("id", id),
					zap.Uint32("payloadType", payloadType),
					zap.String("address", address),
					zap.Error(err),
				)
			}

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
