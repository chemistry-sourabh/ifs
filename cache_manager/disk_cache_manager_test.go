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

package cache_manager

import (
	"github.com/chemistry-sourabh/ifs/communicator"
	"github.com/chemistry-sourabh/ifs/file_op_executor"
	"github.com/chemistry-sourabh/ifs/ifstest"
	"github.com/chemistry-sourabh/ifs/structures"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"
)

func TestDiskCacheManager_Open(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	f, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	ifstest.Compare(t, f.Name(), path.Join(cachePath, "1"))

	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)
	ifstest.Compare(t, len(data), 100)

	err = f.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Open2(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	f, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	err = f.Close()
	ifstest.Ok(t, err)

	f, err = dcm.Open(rp, os.O_RDONLY)
	ifstest.Ok(t, err)

	data1, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Open3(t *testing.T) {
	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentAddress := "127.0.0.1:5002"
	cachePath := "/tmp/test_cache"

	foe := file_op_executor.NewRemoteFileOpExecutor()
	foe.Receiver = communicator.NewAgentZmqReceiver()
	go foe.Run(agentAddress)

	ifstest.CreateTempFile("test")
	ifstest.WriteDummyData("test", 1000)

	time.Sleep(time.Second)

	dcm := NewDiskCacheManager()
	dcm.Sender = communicator.NewFsZmqSender(clientAddress)
	dcm.Sender.Connect([]string{agentAddress})
	dcm.Run(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "127.0.0.1",
		Port:     5002,
		Path:     "/tmp/test",
	}

	f, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	ifstest.Compare(t, f.Name(), path.Join(cachePath, "1"))

	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)
	ifstest.Compare(t, len(data), 1000)

	err = f.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")
}

func TestDiskCacheManager_Rename(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	f, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	err = f.Close()
	ifstest.Ok(t, err)

	err = dcm.Rename(rp, "/tmp/test1")
	ifstest.Ok(t, err)

	rp.Path = "/tmp/test1"

	f, err = dcm.Open(rp, os.O_RDONLY)
	ifstest.Ok(t, err)

	data1, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Rename2(t *testing.T) {
	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentAddress := "127.0.0.1:5014"
	cachePath := "/tmp/test_cache"

	foe := file_op_executor.NewRemoteFileOpExecutor()
	foe.Receiver = communicator.NewAgentZmqReceiver()
	go foe.Run(agentAddress)

	ifstest.CreateTempFile("test")
	ifstest.WriteDummyData("test", 1000)

	time.Sleep(time.Second)

	dcm := NewDiskCacheManager()
	dcm.Sender = communicator.NewFsZmqSender(clientAddress)
	dcm.Sender.Connect([]string{agentAddress})
	dcm.Run(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "127.0.0.1",
		Port:     5014,
		Path:     "/tmp/test",
	}

	f, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	err = f.Close()
	ifstest.Ok(t, err)

	err = dcm.Rename(rp, "/tmp/test1")
	ifstest.Ok(t, err)

	rp.Path = "/tmp/test1"

	f, err = dcm.Open(rp, os.O_RDONLY)
	ifstest.Ok(t, err)

	data1, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test1")
}
