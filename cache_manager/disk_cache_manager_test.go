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
	"strconv"
	"testing"
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

	fd, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	f := val.(*os.File)
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
	//time.Sleep(ifstest.TEST_WAIT)

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

	fd, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	f := val.(*os.File)
	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	err = f.Close()
	ifstest.Ok(t, err)

	fd, err = dcm.Open(rp, os.O_RDONLY)
	ifstest.Ok(t, err)

	val, _ = dcm.opened.Load(fd)
	f = val.(*os.File)
	data1, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Open3(t *testing.T) {
	//time.Sleep(ifstest.TEST_WAIT)

	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	cachePath := "/tmp/test_cache"

	foe := file_op_executor.NewRemoteFileOpExecutor()
	foe.Receiver = communicator.NewAgentZmqReceiver()
	go foe.Run(agentAddress)

	ifstest.CreateTempFile("test")
	ifstest.WriteDummyData("test", 1000)

	dcm := NewDiskCacheManager()
	dcm.Sender = communicator.NewFsZmqSender(clientAddress)
	dcm.Sender.Connect([]string{agentAddress})
	dcm.Run(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "127.0.0.1",
		Port:     uint32(agentPort),
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	f := val.(*os.File)
	ifstest.Compare(t, f.Name(), path.Join(cachePath, "1"))

	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)
	ifstest.Compare(t, len(data), 1000)

	err = f.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_Rename(t *testing.T) {
	//time.Sleep(ifstest.TEST_WAIT)

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

	fd, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	f := val.(*os.File)
	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	err = f.Close()
	ifstest.Ok(t, err)

	err = dcm.Rename(rp, "/tmp/test1")
	ifstest.Ok(t, err)

	rp.Path = "/tmp/test1"

	fd, err = dcm.Open(rp, os.O_RDONLY)
	ifstest.Ok(t, err)

	val, _ = dcm.opened.Load(fd)
	f = val.(*os.File)
	data1, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Rename2(t *testing.T) {
	//time.Sleep(ifstest.TEST_WAIT)

	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	cachePath := "/tmp/test_cache"

	foe := file_op_executor.NewRemoteFileOpExecutor()
	foe.Receiver = communicator.NewAgentZmqReceiver()
	go foe.Run(agentAddress)

	ifstest.CreateTempFile("test")
	ifstest.WriteDummyData("test", 1000)

	dcm := NewDiskCacheManager()
	dcm.Sender = communicator.NewFsZmqSender(clientAddress)
	dcm.Sender.Connect([]string{agentAddress})
	dcm.Run(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "127.0.0.1",
		Port:     uint32(agentPort),
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	f := val.(*os.File)
	data, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	err = f.Close()
	ifstest.Ok(t, err)

	err = dcm.Rename(rp, "/tmp/test1")
	ifstest.Ok(t, err)

	rp.Path = "/tmp/test1"

	fd, err = dcm.Open(rp, os.O_RDONLY)
	ifstest.Ok(t, err)

	val, _ = dcm.opened.Load(fd)
	f = val.(*os.File)
	data1, err := ioutil.ReadAll(f)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test1")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_Create(t *testing.T) {
	//time.Sleep(ifstest.TEST_WAIT)

	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	dirPath := &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp",
	}

	fd, err := dcm.Create(dirPath, "test")
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	f := val.(*os.File)

	ifstest.Compare(t, f.Name(), path.Join(cachePath, "1"))

	err = f.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Create2(t *testing.T) {
	//time.Sleep(ifstest.TEST_WAIT)

	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	cachePath := "/tmp/test_cache"

	foe := file_op_executor.NewRemoteFileOpExecutor()
	foe.Receiver = communicator.NewAgentZmqReceiver()
	go foe.Run(agentAddress)

	dcm := NewDiskCacheManager()
	dcm.Sender = communicator.NewFsZmqSender(clientAddress)
	dcm.Sender.Connect([]string{agentAddress})
	dcm.Run(cachePath, 100)

	dirPath := &structures.RemotePath{
		Hostname: "127.0.0.1",
		Port:     uint32(agentPort),
		Path:     "/tmp",
	}

	fd, err := dcm.Create(dirPath, "test")
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	f := val.(*os.File)

	err = f.Close()
	ifstest.Ok(t, err)

	_, err = os.Stat("/tmp/test")
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_Remove(t *testing.T) {
	//time.Sleep(ifstest.TEST_WAIT)

	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	dirPath := &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp",
	}

	fd, err := dcm.Create(dirPath, "test")
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	f := val.(*os.File)

	err = f.Close()
	ifstest.Ok(t, err)

	filePath := &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	err = dcm.Remove(filePath)

	_, err = os.Stat(path.Join(cachePath, "1"))
	ifstest.Err(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Remove2(t *testing.T) {
	//time.Sleep(ifstest.TEST_WAIT)

	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	cachePath := "/tmp/test_cache"

	foe := file_op_executor.NewRemoteFileOpExecutor()
	foe.Receiver = communicator.NewAgentZmqReceiver()
	go foe.Run(agentAddress)

	dcm := NewDiskCacheManager()
	dcm.Sender = communicator.NewFsZmqSender(clientAddress)
	dcm.Sender.Connect([]string{agentAddress})
	dcm.Run(cachePath, 100)

	dirPath := &structures.RemotePath{
		Hostname: "127.0.0.1",
		Port:     uint32(agentPort),
		Path:     "/tmp",
	}

	fd, err := dcm.Create(dirPath, "test")
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	f := val.(*os.File)

	err = f.Close()
	ifstest.Ok(t, err)

	filePath := &structures.RemotePath{
		Hostname: "127.0.0.1",
		Port:     uint32(agentPort),
		Path:     "/tmp/test",
	}

	err = dcm.Remove(filePath)

	_, err = os.Stat("/tmp/test")
	ifstest.Err(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_Close(t *testing.T) {
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

	fd, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	err = dcm.Close(rp, fd)
	ifstest.Ok(t, err)

	_, ok := dcm.opened.Load(fd)
	ifstest.Compare(t, ok, false)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

}

func TestDiskCacheManager_Close2(t *testing.T) {
	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	cachePath := "/tmp/test_cache"

	foe := file_op_executor.NewRemoteFileOpExecutor()
	foe.Receiver = communicator.NewAgentZmqReceiver()
	go foe.Run(agentAddress)

	ifstest.CreateTempFile("test")

	dcm := NewDiskCacheManager()
	dcm.Sender = communicator.NewFsZmqSender(clientAddress)
	dcm.Sender.Connect([]string{agentAddress})
	dcm.Run(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "127.0.0.1",
		Port:     uint32(agentPort),
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, os.O_RDWR)
	ifstest.Ok(t, err)

	err = dcm.Close(rp, fd)
	ifstest.Ok(t, err)

	_, ok := dcm.opened.Load(fd)
	ifstest.Compare(t, ok, false)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_Truncate(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp",
	}

	fd, err := dcm.Create(rp, "test")
	ifstest.Ok(t, err)

	err = dcm.Close(rp, fd)
	ifstest.Ok(t, err)

	ifstest.WriteDummyData("test_cache/1", 1000)

	f, err := os.Stat("/tmp/test_cache/1")
	ifstest.Ok(t, err)
	ifstest.Compare(t, f.Size(), int64(1000))

	rp = &structures.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	err = dcm.Truncate(rp, 100)
	ifstest.Ok(t, err)

	f, err = os.Stat("/tmp/test_cache/1")
	ifstest.Ok(t, err)
	ifstest.Compare(t, f.Size(), int64(100))

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

}

func TestDiskCacheManager_Truncate2(t *testing.T) {
	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	cachePath := "/tmp/test_cache"

	foe := file_op_executor.NewRemoteFileOpExecutor()
	foe.Receiver = communicator.NewAgentZmqReceiver()
	go foe.Run(agentAddress)

	ifstest.CreateTempFile("test")
	ifstest.WriteDummyData("test", 1000)

	dcm := NewDiskCacheManager()
	dcm.Sender = communicator.NewFsZmqSender(clientAddress)
	dcm.Sender.Connect([]string{agentAddress})
	dcm.Run(cachePath, 100)

	rp := &structures.RemotePath{
		Hostname: "127.0.0.1",
		Port:     uint32(agentPort),
		Path:     "/tmp/test",
	}

	err := dcm.Truncate(rp, 100)

	stat, err := os.Stat("/tmp/test")
	ifstest.Ok(t, err)

	ifstest.Compare(t, stat.Size(), int64(100))

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}