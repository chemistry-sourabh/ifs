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
	"github.com/chemistry-sourabh/ifs/structure"
)

type Sender interface {
	Connect(endpoints []string)
	Disconnect()
	SendRequest(op uint32, address string, payload *structure.RequestPayload) (*structure.ReplyPayload, error)
}

type Receiver interface {
	Bind(address string) error
	Unbind()
	RecvRequest() (uint64, uint32, *structure.RequestPayload, error)
	SendReply(id uint64, payloadType uint32, payload *structure.ReplyPayload) error
}
