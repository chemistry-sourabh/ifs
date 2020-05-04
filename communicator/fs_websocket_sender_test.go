/*
 * Copyright 2020 Sourabh Bollapragada
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
	"github.com/chemistry-sourabh/ifs/ifstest"
	"github.com/chemistry-sourabh/ifs/structure"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestFsWebSocketSender_Comm(t *testing.T) {
	t.SkipNow()

	ifstest.SetupLogger()
	agent1Address := "127.0.0.1:" + strconv.Itoa(int(ifstest.GetOpenPort()))
	agent2Address := "127.0.0.1:" + strconv.Itoa(int(ifstest.GetOpenPort()))
	agent3Address := "127.0.0.1:" + strconv.Itoa(int(ifstest.GetOpenPort()))

	addresses := []string{
		agent1Address,
		agent2Address,
		agent3Address,
	}

	fws := NewFsWebSocketSender()
	ftr1 := &FsZmqTestReceiver{}
	ftr2 := &FsZmqTestReceiver{}
	ftr3 := &FsZmqTestReceiver{}

	ftr1.Bind(agent1Address)
	ftr2.Bind(agent2Address)
	ftr3.Bind(agent3Address)

	fws.Connect(addresses)

	fm := &structure.FetchMessage{
		Path: "/tmp/test",
	}

	msg := &structure.RequestPayload{
		Payload: &structure.RequestPayload_FetchMsg{
			FetchMsg: fm,
		},
	}

	r := rand.New(rand.NewSource(time.Now().Unix()))

	for i := 0; i < 10000; i++ {
		index := r.Intn(len(addresses))
		_, err := fws.SendRequest(structure.FetchMessageCode, addresses[index], msg)
		ifstest.Ok(t, err)
	}

	fws.Disconnect()
	ftr1.Unbind()
	ftr2.Unbind()
	ftr3.Unbind()
}
