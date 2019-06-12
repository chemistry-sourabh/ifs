///*
//Copyright 2018 Sourabh Bollapragada
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//*/
//
package ifs

import (
	"github.com/chemistry-sourabh/ifs/communicator"
	"github.com/chemistry-sourabh/ifs/file_op_executor"
	"go.uber.org/zap"
	"strconv"
)

func StartAgent(address string, port uint16) {

	zap.L().Info("Starting Agent",
		zap.String("address", address),
		zap.Uint16("port", port),
	)

	recv := communicator.NewAgentZmqReceiver()

	foe := file_op_executor.RemoteFileOpExecutor{}
	foe.Receiver = recv

	foe.Run(address + ":" + strconv.Itoa(int(port)))

}
