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
	"context"
	"github.com/chemistry-sourabh/ifs/structure"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
)

type FsTestWebSocketReceiver struct {
	server *http.Server
	conn   *websocket.Conn
}

func (ftr *FsTestWebSocketReceiver) recvMessages() {
	zap.L().Info("Listening For Messages")

	for {
		messageType, data, err := ftr.conn.ReadMessage()

		if err != nil {

			zap.L().Warn("Listening Failed",
				zap.Error(err),
			)
		}

		if messageType == websocket.BinaryMessage {

			request := &structure.Request{}

			err = proto.Unmarshal(data, request)

			if err != nil {
				zap.L().Fatal("Unmarshalling Failed",
					zap.Error(err),
				)
			}

			zap.L().Debug("Received Message",
				zap.Uint64("id", request.Id),
				zap.Uint32("type", request.PayloadType),
			)

			reply := &structure.Request{}

			reply.Id = request.Id
			reply.PayloadType = structure.DataMessageCode

			data, err = proto.Marshal(reply)

			if err != nil {
				zap.L().Error("Couldnt Marshall",
					zap.Error(err),
				)
			}

			err = ftr.conn.WriteMessage(websocket.BinaryMessage, data)

			if err != nil {
				zap.L().Error("Couldn't Send",
					zap.Error(err),
				)
			}

			zap.L().Debug("Sent Message",
				zap.Uint64("Id", reply.Id),
				zap.Uint32("Type", reply.PayloadType),
			)
		}
	}
}

func (ftr *FsTestWebSocketReceiver) handleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	upgrader.EnableCompression = true
	conn, _ := upgrader.Upgrade(w, r, nil)
	ftr.conn = conn

	go ftr.recvMessages()
}

func (ftr *FsTestWebSocketReceiver) startServer() {
	err := ftr.server.ListenAndServe()

	if err != nil && err != http.ErrServerClosed {
		zap.L().Fatal("Listen and Serve Failed",
			zap.Error(err),
		)
	}
}

func (ftr *FsTestWebSocketReceiver) Bind(address string) {

	mux := http.NewServeMux()
	mux.HandleFunc("/", ftr.handleRequests)

	server := &http.Server{
		Addr: address,
		Handler: mux,
	}
	ftr.server = server

	go ftr.startServer()
}

func (ftr *FsTestWebSocketReceiver) Unbind() {

	err := ftr.conn.Close()

	if err != nil {
		zap.L().Fatal("Failed to Close Connection",
			zap.Error(err),
		)
	}

	err = ftr.server.Shutdown(context.TODO())

	if err != nil {
		zap.L().Fatal("Failed to Shutdown Server",
			zap.Error(err),
		)
	}
}
