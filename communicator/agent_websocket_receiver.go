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
	"context"
	"github.com/chemistry-sourabh/ifs/structure"
	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"net/http"
)

type AgentWebSocketReceiver struct {
	server *http.Server
	conn   *websocket.Conn
	recv   chan *structure.Request
	send   chan []byte
}

func NewAgentWebSocketReceiver() *AgentWebSocketReceiver {
	return &AgentWebSocketReceiver{
		send: make(chan []byte, structure.ChannelLength),
		recv: make(chan *structure.Request, structure.ChannelLength),
	}
}

func (awr *AgentWebSocketReceiver) recvMessages() {

	for {
		zap.L().Debug("Listening for Message",
			zap.String("address", awr.conn.RemoteAddr().String()),
		)

		messageType, data, err := awr.conn.ReadMessage()

		if err != nil {
			zap.L().Warn("Read Message Failed",
				zap.Error(err),
			)
			break
		}

		if messageType == websocket.BinaryMessage {
			request := &structure.Request{}

			err := proto.Unmarshal(data, request)

			if err != nil {
				zap.L().Fatal("Unmarshalling Failed",
					zap.Error(err),
				)
			}

			zap.L().Debug("Received Message",
				zap.Uint64("id", request.Id),
				zap.Uint32("type", request.PayloadType),
			)

			awr.recv <- request
		}
	}

	awr.conn = nil
	close(awr.recv)
}

func (awr *AgentWebSocketReceiver) sendMessages() {
	for data := range awr.send {
		err := awr.conn.WriteMessage(websocket.BinaryMessage, data)

		if err != nil {
			zap.L().Fatal("Failed to Send Reply",
				zap.Error(err),
			)
		}
	}
}

func (awr *AgentWebSocketReceiver) handleRequests(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	upgrader.EnableCompression = true
	conn, _ := upgrader.Upgrade(w, r, nil)
	awr.conn = conn

	go awr.recvMessages()
	go awr.sendMessages()
}

func (awr *AgentWebSocketReceiver) startServer() {
	err := awr.server.ListenAndServe()

	if err != nil && err != http.ErrServerClosed {
		zap.L().Fatal("Listen and Serve Failed",
			zap.Error(err),
		)
	}
}

func (awr *AgentWebSocketReceiver) Bind(address string) error {

	mux := http.NewServeMux()
	mux.HandleFunc("/", awr.handleRequests)

	server := &http.Server{
		Addr:    address,
		Handler: mux,
	}
	awr.server = server

	go awr.startServer()

	return nil
}

func (awr *AgentWebSocketReceiver) Unbind() {
	var err error
	if awr.conn != nil {
		err = awr.conn.Close()
	}

	if err != nil {
		zap.L().Fatal("Failed to Close Connection",
			zap.Error(err),
		)
	}

	err = awr.server.Shutdown(context.TODO())

	if err != nil {
		zap.L().Fatal("Failed to Shutdown Server",
			zap.Error(err),
		)
	}
}

func (awr *AgentWebSocketReceiver) RecvRequest() (uint64, uint32, *structure.RequestPayload, error) {
	request, ok := <-awr.recv

	if ok {
		return request.Id, request.PayloadType, request.Payload, nil
	} else {
		awr.recv = make(chan *structure.Request, structure.ChannelLength)
		return 0, 0, nil, errors.New("Channel Closed")
	}
}

func (awr *AgentWebSocketReceiver) SendReply(id uint64, payloadType uint32, payload *structure.ReplyPayload) error {
	reply := &structure.Reply{
		Id:          id,
		PayloadType: payloadType,
		Payload:     payload,
	}

	data, err := proto.Marshal(reply)

	if err != nil {
		return err
	}

	awr.send <- data

	return nil
}
