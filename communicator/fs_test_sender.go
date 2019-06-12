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

package communicator

import (
	"crypto/rand"
	"github.com/chemistry-sourabh/ifs/structure"
)

type FsTestSender struct {
}

func (tnm *FsTestSender) SendRequest(payloadType uint32, address string, payload *structure.RequestPayload) (*structure.ReplyPayload, error) {
	switch payloadType {

	case structure.FetchMessageCode:
		b := make([]byte, 100)
		_, err := rand.Read(b)

		if err != nil {
			return nil, err
		}

		msg := &structure.DataMessage{
			Data: b,
		}

		payload := &structure.ReplyPayload{
			Payload: &structure.ReplyPayload_DataMsg{
				DataMsg: msg,
			},
		}

		return payload, nil
	case structure.OpenMessageCode, structure.RenameMessageCode, structure.CreateMessageCode,
		structure.RemoveMessageCode, structure.CloseMessageCode, structure.TruncateMessageCode,
		structure.FlushMessageCode:
		payload := &structure.ReplyPayload{}
		return payload, nil
	case structure.ReadMessageCode:
		rm := payload.GetReadMsg()
		b := make([]byte, rm.Size)
		_, err := rand.Read(b)

		if err != nil {
			return nil, err
		}

		msg := &structure.DataMessage{
			Data: b,
		}

		payload := &structure.ReplyPayload{
			Payload: &structure.ReplyPayload_DataMsg{
				DataMsg: msg,
			},
		}

		return payload, nil
	case structure.WriteMessageCode:
		wm := payload.GetWriteMsg()

		msg := &structure.WriteOkMessage{
			Size:     uint64(len(wm.GetData())),
			FileSize: uint64(len(wm.GetData())),
		}

		payload := &structure.ReplyPayload{
			Payload: &structure.ReplyPayload_WriteOkMsg{
				WriteOkMsg: msg,
			},
		}

		return payload, nil
	case structure.AttrMessageCode:
		msg := &structure.FileInfoMessage{
			Size:  1000,
			Mode:  2000,
			Mtime: 3000,
		}

		payload := &structure.ReplyPayload{
			Payload: &structure.ReplyPayload_FileInfoMsg{
				FileInfoMsg: msg,
			},
		}

		return payload, nil

	case structure.ReadDirMessageCode:
		msg1 := &structure.FileInfoMessage{
			Size:  1000,
			Mode:  2000,
			Mtime: 3000,
			IsDir: false,
		}

		msg2 := &structure.FileInfoMessage{
			Size:  4000,
			Mode:  5000,
			Mtime: 6000,
			IsDir: false,
		}

		msg := &structure.FileInfosMessage{
			FileInfos: []*structure.FileInfoMessage{msg1, msg2},
		}

		payload := &structure.ReplyPayload{
			Payload: &structure.ReplyPayload_FileInfosMsg{
				FileInfosMsg: msg,
			},
		}

		return payload, nil
	}

	return nil, nil
}

func (tnm *FsTestSender) Connect(endpoints []string) {
	// Connected!!
}

func (tnm *FsTestSender) Disconnect() {
	// Disconnected!!
}
