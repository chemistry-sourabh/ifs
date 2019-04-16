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

package network_manager

import (
	"crypto/rand"
	"github.com/chemistry-sourabh/ifs/structures"
	"github.com/golang/protobuf/proto"
)

type TestNetworkManager struct {

}

func (tnm *TestNetworkManager) SendMessage(op uint32, msg proto.Message) (proto.Message, error) {
	if op == structures.FetchMessageCode {
		b := make([]byte, 100)
		_, err := rand.Read(b)

		if err != nil {
			return nil, err
		}

		msg := &structures.FileMessage{
			File: b,
		}

		return msg, nil
	}

	return nil, nil
}
