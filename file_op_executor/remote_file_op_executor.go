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
	"github.com/chemistry-sourabh/ifs/structure"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"syscall"
	"time"
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

func (foe *RemoteFileOpExecutor) createErrMsg(err error) *structure.ReplyPayload {
	return &structure.ReplyPayload{
		Payload: &structure.ReplyPayload_ErrMsg{
			ErrMsg: &structure.ErrMessage{
				Error: err.Error(),
			},
		},
	}
}

// TODO Check Compression
func (foe *RemoteFileOpExecutor) fetch(req *structure.FetchMessage) (*structure.DataMessage, error) {

	zap.L().Debug("Processing Fetch Message",
		zap.String("path", req.GetPath()),
	)

	dataMsg := &structure.DataMessage{}
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

func (foe *RemoteFileOpExecutor) open(req *structure.OpenMessage) error {

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

func (foe *RemoteFileOpExecutor) rename(req *structure.RenameMessage) error {
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

func (foe *RemoteFileOpExecutor) create(req *structure.CreateMessage) error {
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

func (foe *RemoteFileOpExecutor) mkdir(req *structure.MkdirMessage) error {
	filePath := path.Join(req.GetBaseDir(), req.GetName())

	zap.L().Debug("Processing Mkdir Message",
		zap.String("path", filePath),
	)

	err := os.Mkdir(filePath, os.FileMode(0755))
	if err != nil {

		zap.L().Error("Mkdir Error",
			zap.String("path", filePath),
			zap.Error(err),
		)
		return err
	}

	return nil
}

func (foe *RemoteFileOpExecutor) remove(req *structure.RemoveMessage) error {
	zap.L().Debug("Processing Remove Message",
		zap.String("path", req.GetPath()),
		zap.Bool("is-dir", req.GetIsDir()),
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

func (foe *RemoteFileOpExecutor) close(req *structure.CloseMessage) error {
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

func (foe *RemoteFileOpExecutor) flush(req *structure.FlushMessage) error {
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

func (foe *RemoteFileOpExecutor) truncate(req *structure.TruncateMessage) error {
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

func (foe *RemoteFileOpExecutor) read(req *structure.ReadMessage) (*structure.DataMessage, error) {
	zap.L().Debug("Processing Read Message",
		zap.Uint64("fd", req.GetFd()),
		zap.Uint64("offset", req.GetOffset()),
		zap.Uint64("size", req.GetSize()),
	)

	val, ok := foe.fp.Load(req.GetFd())

	if !ok {

		zap.L().Error("Read Error",
			zap.Uint64("fd", req.GetFd()),
			zap.Uint64("offset", req.GetOffset()),
			zap.Uint64("size", req.GetSize()),
			zap.Error(os.ErrInvalid),
		)

		return nil, os.ErrInvalid
	}

	f := val.(*os.File)
	data := make([]byte, req.GetSize())
	_, err := f.ReadAt(data, int64(req.GetOffset()))

	if err != nil {

		zap.L().Error("Read Error",
			zap.Uint64("fd", req.GetFd()),
			zap.Uint64("offset", req.GetOffset()),
			zap.Uint64("size", req.GetSize()),
			zap.Error(err),
		)

		return nil, err
	}

	dm := &structure.DataMessage{
		Data: data,
	}

	return dm, nil
}

func (foe *RemoteFileOpExecutor) write(req *structure.WriteMessage) (*structure.WriteOkMessage, error) {
	zap.L().Debug("Processing Write Message",
		zap.Uint64("fd", req.GetFd()),
		zap.Uint64("offset", req.GetOffset()),
		zap.Uint64("size", uint64(len(req.GetData()))),
	)

	val, ok := foe.fp.Load(req.GetFd())

	if !ok {

		zap.L().Error("Write Error",
			zap.Uint64("fd", req.GetFd()),
			zap.Uint64("offset", req.GetOffset()),
			zap.Uint64("size", uint64(len(req.GetData()))),
			zap.Error(os.ErrInvalid),
		)

		return nil, os.ErrInvalid
	}

	f := val.(*os.File)
	size, err := f.WriteAt(req.GetData(), int64(req.GetOffset()))

	if err != nil {

		zap.L().Error("Write Error",
			zap.Uint64("fd", req.GetFd()),
			zap.Uint64("offset", req.GetOffset()),
			zap.Uint64("size", uint64(len(req.GetData()))),
			zap.Error(err),
		)

		return nil, err
	}

	fi, err := f.Stat()

	if err != nil {

		zap.L().Error("Write Error",
			zap.Uint64("fd", req.GetFd()),
			zap.Uint64("offset", req.GetOffset()),
			zap.Uint64("size", uint64(len(req.GetData()))),
			zap.Error(err),
		)

		return nil, err
	}


	wm := &structure.WriteOkMessage{
		Size: uint64(size),
		FileSize: uint64(fi.Size()),
	}

	return wm, nil
}

func (foe *RemoteFileOpExecutor) attr(req *structure.AttrMessage) (*structure.FileInfoMessage, error) {
	zap.L().Debug("Processing Attr Message",
		zap.String("Path", req.GetPath()),
	)

	fi, err := os.Stat(req.Path)

	if err != nil {

		zap.L().Error("Attr Error",
			zap.String("Path", req.GetPath()),
			zap.Error(err),
		)

		return nil, err
	}

	// TODO Get Atime in a platform independent way
	fim := &structure.FileInfoMessage{
		Name: fi.Name(),
		Size: uint64(fi.Size()),
		Mode: uint32(fi.Mode()),
		Mtime: uint64(fi.ModTime().UnixNano()),
		Atime: uint64(fi.ModTime().UnixNano()),
		IsDir: fi.IsDir(),
	}

	return fim, nil
}

func (foe *RemoteFileOpExecutor) readDir(req *structure.ReadDirMessage) (*structure.FileInfosMessage, error) {
	zap.L().Debug("Processing ReadDir Message",
		zap.String("Path", req.GetPath()),
	)

	fileInfos, err := ioutil.ReadDir(req.GetPath())

	if err != nil {
		return nil, err
	}

	var fileInfoMessages []*structure.FileInfoMessage

	for _, fi := range fileInfos {
		fim := &structure.FileInfoMessage {
			Name: fi.Name(),
			Size: uint64(fi.Size()),
			Mode: uint32(fi.Mode()),
			Mtime: uint64(fi.ModTime().UnixNano()),
			Atime: uint64(fi.ModTime().UnixNano()),
			IsDir: fi.IsDir(),
		}

		fileInfoMessages = append(fileInfoMessages, fim)
	}

	fileInfosMessage := &structure.FileInfosMessage{
		FileInfos: fileInfoMessages,
	}

	return fileInfosMessage, nil
}

func (foe *RemoteFileOpExecutor) setMode(req *structure.SetModeMessage) error {
	zap.L().Debug("Processing SetMode Message",
		zap.String("path", req.GetPath()),
		zap.String("mode", os.FileMode(req.GetMode()).String()),
	)

	err := os.Chmod(req.GetPath(), os.FileMode(req.GetMode()))

	if err != nil {

		zap.L().Error("SetMode Error",
			zap.String("path", req.GetPath()),
			zap.String("mode", os.FileMode(req.GetMode()).String()),
			zap.Error(err),
		)

		return err
	}

	return nil
}

func (foe *RemoteFileOpExecutor) setMtime(req *structure.SetMtimeMessage) error {
	zap.L().Debug("Processing SetMtime Message",
		zap.String("path", req.GetPath()),
		zap.String("mtime", time.Unix(0, int64(req.GetMtime())).String()),
		zap.String("atime", time.Unix(0, int64(req.GetAtime())).String()),
	)

	err := os.Chtimes(req.GetPath(), time.Unix(0, int64(req.GetAtime())), time.Unix(0, int64(req.GetMtime())))

	if err != nil {

		zap.L().Error("SetMtime Error",
			zap.String("path", req.GetPath()),
			zap.String("mtime", time.Unix(0, int64(req.GetMtime())).String()),
			zap.String("atime", time.Unix(0, int64(req.GetAtime())).String()),
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
		case structure.FetchMessageCode, structure.ReadMessageCode:
			go func() {
				var payload *structure.DataMessage
				var err error
				switch payloadType {
				case structure.FetchMessageCode:
					payload, err = foe.fetch(req.GetFetchMsg())
				case structure.ReadMessageCode:
					payload, err = foe.read(req.GetReadMsg())
				}

				var reply *structure.ReplyPayload
				var replyType uint32 = structure.ErrMessageCode
				if err != nil {
					reply = foe.createErrMsg(err)
				} else {

					reply = &structure.ReplyPayload{
						Payload: &structure.ReplyPayload_DataMsg{
							DataMsg: payload,
						},
					}

					replyType = structure.DataMessageCode

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

		case structure.OpenMessageCode, structure.RenameMessageCode, structure.CreateMessageCode,
			structure.RemoveMessageCode, structure.CloseMessageCode, structure.TruncateMessageCode,
			structure.FlushMessageCode, structure.MkdirMessageCode, structure.SetModeMessageCode,
			structure.SetMtimeMessageCode:
			go func() {
				var err error
				switch payloadType {
				case structure.OpenMessageCode:
					err = foe.open(req.GetOpenMsg())
				case structure.RenameMessageCode:
					err = foe.rename(req.GetRenameMsg())
				case structure.CreateMessageCode:
					err = foe.create(req.GetCreateMsg())
				case structure.RemoveMessageCode:
					err = foe.remove(req.GetRemoveMsg())
				case structure.CloseMessageCode:
					err = foe.close(req.GetCloseMsg())
				case structure.TruncateMessageCode:
					err = foe.truncate(req.GetTruncateMsg())
				case structure.FlushMessageCode:
					err = foe.flush(req.GetFlushMsg())
				case structure.MkdirMessageCode:
					err = foe.mkdir(req.GetMkdirMsg())
				case structure.SetModeMessageCode:
					err = foe.setMode(req.GetSetModeMsg())
				case structure.SetMtimeMessageCode:
					err = foe.setMtime(req.GetSetMtimeMsg())
				}

				var reply *structure.ReplyPayload
				var replyType uint32 = structure.ErrMessageCode
				if err != nil {
					reply = foe.createErrMsg(err)
				} else {

					reply = &structure.ReplyPayload{}
					replyType = structure.OkMessageCode

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
		case structure.WriteMessageCode:
			go func() {
				payload, err := foe.write(req.GetWriteMsg())

				var reply *structure.ReplyPayload
				var replyType uint32 = structure.ErrMessageCode
				if err != nil {
					reply = foe.createErrMsg(err)
				} else {

					reply = &structure.ReplyPayload{
						Payload: &structure.ReplyPayload_WriteOkMsg{
							WriteOkMsg: payload,
						},
					}

					replyType = structure.WriteOkMessageCode

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
		case structure.AttrMessageCode:
			go func() {
				payload, err := foe.attr(req.GetAttrMsg())

				var reply *structure.ReplyPayload
				var replyType uint32 = structure.ErrMessageCode
				if err != nil {
					reply = foe.createErrMsg(err)
				} else {

					reply = &structure.ReplyPayload{
						Payload: &structure.ReplyPayload_FileInfoMsg{
							FileInfoMsg: payload,
						},
					}

					replyType = structure.FileInfoMessageCode

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
		case structure.ReadDirMessageCode:
			go func() {
				payload, err := foe.readDir(req.GetReadDirMsg())

				var reply *structure.ReplyPayload
				var replyType uint32 = structure.ErrMessageCode
				if err != nil {
					reply = foe.createErrMsg(err)
				} else {

					reply = &structure.ReplyPayload{
						Payload: &structure.ReplyPayload_FileInfosMsg{
							FileInfosMsg: payload,
						},
					}

					replyType = structure.FileInfosMessageCode

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
