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
	"crypto/rand"
	"github.com/chemistry-sourabh/ifs/communicator"
	"github.com/chemistry-sourabh/ifs/file_op_executor"
	"github.com/chemistry-sourabh/ifs/ifstest"
	"github.com/chemistry-sourabh/ifs/structure"
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

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_RDWR))
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	ifstest.Compare(t, fh.Fp.Name(), path.Join(cachePath, "1"))

	data, err := ioutil.ReadAll(fh.Fp)
	ifstest.Ok(t, err)
	ifstest.Compare(t, len(data), 100)

	err = fh.Fp.Close()
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

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_RDWR))
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	data, err := ioutil.ReadAll(fh.Fp)
	ifstest.Ok(t, err)

	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	fd, err = dcm.Open(rp, uint32(os.O_RDONLY))
	ifstest.Ok(t, err)

	val, _ = dcm.opened.Load(fd)
	fh = val.(*structure.CacheFileHandle)
	data1, err := ioutil.ReadAll(fh.Fp)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Open3(t *testing.T) {
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

	rp := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_RDWR))
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	ifstest.Compare(t, fh.Fp.Name(), path.Join(cachePath, "1"))

	data, err := ioutil.ReadAll(fh.Fp)
	ifstest.Ok(t, err)
	ifstest.Compare(t, len(data), 1000)

	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_Rename(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_RDWR))
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	data, err := ioutil.ReadAll(fh.Fp)
	ifstest.Ok(t, err)

	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	err = dcm.Rename(rp, "/tmp/test1")
	ifstest.Ok(t, err)

	rp.Path = "/tmp/test1"

	fd, err = dcm.Open(rp, uint32(os.O_RDONLY))
	ifstest.Ok(t, err)

	val, _ = dcm.opened.Load(fd)
	fh = val.(*structure.CacheFileHandle)
	data1, err := ioutil.ReadAll(fh.Fp)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Rename2(t *testing.T) {
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

	rp := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_RDWR))
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	data, err := ioutil.ReadAll(fh.Fp)
	ifstest.Ok(t, err)

	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	err = dcm.Rename(rp, "/tmp/test1")
	ifstest.Ok(t, err)

	rp.Path = "/tmp/test1"

	fd, err = dcm.Open(rp, uint32(os.O_RDONLY))
	ifstest.Ok(t, err)

	val, _ = dcm.opened.Load(fd)
	fh = val.(*structure.CacheFileHandle)
	data1, err := ioutil.ReadAll(fh.Fp)
	ifstest.Ok(t, err)

	ifstest.Compare(t, data1, data)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test1")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_Create(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	dirPath := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp",
	}

	fd, err := dcm.Create(dirPath, "test")
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	ifstest.Compare(t, fh.Fp.Name(), path.Join(cachePath, "1"))

	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Create2(t *testing.T) {
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

	dirPath := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
		Path:     "/tmp",
	}

	fd, err := dcm.Create(dirPath, "test")
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)

	err = fh.Fp.Close()
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
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	dirPath := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp",
	}

	fd, err := dcm.Create(dirPath, "test")
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)

	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	filePath := &structure.RemotePath{
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

	dirPath := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
		Path:     "/tmp",
	}

	fd, err := dcm.Create(dirPath, "test")
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)

	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	filePath := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
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

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_RDWR))
	ifstest.Ok(t, err)

	err = dcm.Close(fd)
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

	rp := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_RDWR))
	ifstest.Ok(t, err)

	err = dcm.Close(fd)
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

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp",
	}

	fd, err := dcm.Create(rp, "test")
	ifstest.Ok(t, err)

	err = dcm.Close(fd)
	ifstest.Ok(t, err)

	ifstest.WriteDummyData("test_cache/1", 1000)

	f, err := os.Stat("/tmp/test_cache/1")
	ifstest.Ok(t, err)
	ifstest.Compare(t, f.Size(), int64(1000))

	rp = &structure.RemotePath{
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

	rp := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
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

func TestDiskCacheManager_Flush(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp",
	}

	fd, err := dcm.Create(rp, "test")
	ifstest.Ok(t, err)

	err = dcm.Flush(fd)
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)

	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

}

func TestDiskCacheManager_Flush2(t *testing.T) {
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

	rp := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
		Path:     "/tmp",
	}

	fd, err := dcm.Create(rp, "test")
	ifstest.Ok(t, err)

	err = dcm.Flush(fd)
	ifstest.Ok(t, err)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_Read(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	ifstest.CreateTempFile("test")

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp",
	}

	fp, err := os.Open("/tmp/test")
	ifstest.Ok(t, err)

	err = fp.Close()
	ifstest.Ok(t, err)

	dcm.opened.Store(uint64(1), &structure.CacheFileHandle{
		FilePath: rp,
		Fp: fp,
	})

	data, err := dcm.Read(uint64(1), uint64(0), uint64(1000))
	ifstest.Ok(t, err)

	ifstest.Compare(t, len(data), 1000)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")
}

func TestDiskCacheManager_Read2(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_RDONLY))
	ifstest.Ok(t, err)

	data, err := dcm.Read(fd, uint64(0), uint64(1000))
	ifstest.Ok(t, err)

	ifstest.Compare(t, len(data), 1000)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Read3(t *testing.T) {
	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	cachePath := "/tmp/test_cache"

	foe := file_op_executor.NewRemoteFileOpExecutor()
	foe.Receiver = communicator.NewAgentZmqReceiver()
	go foe.Run(agentAddress)

	ifstest.CreateTempFile("test")
	fileData := ifstest.WriteDummyData("test", 1000)

	dcm := NewDiskCacheManager()
	dcm.Sender = communicator.NewFsZmqSender(clientAddress)
	dcm.Sender.Connect([]string{agentAddress})
	dcm.Run(cachePath, 100)

	rp := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_RDWR))
	ifstest.Ok(t, err)

	data, err := dcm.Read(fd, uint64(0), uint64(1000))
	ifstest.Ok(t, err)

	ifstest.Compare(t, data, fileData)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_Write(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_WRONLY))
	ifstest.Ok(t, err)

	data := make([]byte, 1000)
	_, err = rand.Read(data)


	size, err := dcm.Write(fd, uint64(0), data)
	ifstest.Ok(t, err)

	ifstest.Compare(t, size, 1000)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	fileData, err := ioutil.ReadFile("/tmp/test_cache/1")
	ifstest.Ok(t, err)

	ifstest.Compare(t, fileData, data)


	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Write2(t *testing.T) {
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

	rp := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
		Path:     "/tmp/test",
	}

	fd, err := dcm.Open(rp, uint32(os.O_RDWR))
	ifstest.Ok(t, err)

	data := make([]byte, 1000)
	_, err = rand.Read(data)
	ifstest.Ok(t, err)

	size, err := dcm.Write(fd, uint64(0), data)
	ifstest.Ok(t, err)

	ifstest.Compare(t, size, 1000)

	val, _ := dcm.opened.Load(fd)
	fh := val.(*structure.CacheFileHandle)
	err = fh.Fp.Close()
	ifstest.Ok(t, err)

	fileData, err := ioutil.ReadFile("/tmp/test")
	ifstest.Ok(t, err)
	
	ifstest.Compare(t, data, fileData)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_Attr(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	fi, err := dcm.Attr(rp)
	ifstest.Ok(t, err)

	ifstest.Compare(t, fi.Size, uint64(1000))
	ifstest.Compare(t, fi.Mode, uint32(2000))
	ifstest.Compare(t, fi.Mtime, uint64(3000))

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_Attr2(t *testing.T) {
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

	rp := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
		Path:     "/tmp/test",
	}

	fi, err := dcm.Attr(rp)
	ifstest.Ok(t, err)

	stat, err := os.Stat("/tmp/test")
	ifstest.Ok(t, err)

	ifstest.Compare(t, fi.Size, uint64(stat.Size()))
	ifstest.Compare(t, fi.Mode, uint32(stat.Mode()))
	ifstest.Compare(t, fi.Mtime, uint64(stat.ModTime().UnixNano()))
	ifstest.Compare(t, fi.IsDir, false)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}

func TestDiskCacheManager_ReadDir(t *testing.T) {
	ifstest.SetupLogger()
	cachePath := "/tmp/test_cache"

	dcm := NewDiskCacheManager()
	dcm.Sender = &communicator.FsTestSender{}
	dcm.Run(cachePath, 100)

	rp := &structure.RemotePath{
		Hostname: "localhost",
		Port:     8000,
		Path:     "/tmp/test",
	}

	fileInfos, err := dcm.ReadDir(rp)
	ifstest.Ok(t, err)

	ifstest.Compare(t, fileInfos[0].Size, uint64(1000))
	ifstest.Compare(t, fileInfos[0].Mode, uint32(2000))
	ifstest.Compare(t, fileInfos[0].Mtime, uint64(3000))
	ifstest.Compare(t, fileInfos[0].IsDir, false)

	ifstest.Compare(t, fileInfos[1].Size, uint64(4000))
	ifstest.Compare(t, fileInfos[1].Mode, uint32(5000))
	ifstest.Compare(t, fileInfos[1].Mtime, uint64(6000))
	ifstest.Compare(t, fileInfos[1].IsDir, false)

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)
}

func TestDiskCacheManager_ReadDir2(t *testing.T) {
	ifstest.SetupLogger()
	clientAddress := "127.0.0.1:5000"
	agentPort := ifstest.GetOpenPort()
	agentAddress := "127.0.0.1:" + strconv.Itoa(int(agentPort))
	cachePath := "/tmp/test_cache"

	foe := file_op_executor.NewRemoteFileOpExecutor()
	foe.Receiver = communicator.NewAgentZmqReceiver()
	go foe.Run(agentAddress)

	os.MkdirAll("/tmp/test2", 0755)
	ifstest.CreateTempFile("test2/file1")
	ifstest.WriteDummyData("test2/file1", 1000)
	ifstest.CreateTempFile("test2/file2")
	ifstest.WriteDummyData("test2/file2", 2000)

	dcm := NewDiskCacheManager()
	dcm.Sender = communicator.NewFsZmqSender(clientAddress)
	dcm.Sender.Connect([]string{agentAddress})
	dcm.Run(cachePath, 100)

	rp := &structure.RemotePath{
		Hostname: "127.0.0.1",
		Port:     agentPort,
		Path:     "/tmp/test2",
	}

	fi, err := dcm.ReadDir(rp)
	ifstest.Ok(t, err)

	stats, err := ioutil.ReadDir("/tmp/test2")
	ifstest.Ok(t, err)

	for i := range stats {
		ifstest.Compare(t, fi[i].Size, uint64(stats[i].Size()))
		ifstest.Compare(t, fi[i].Mode, uint32(stats[i].Mode()))
		ifstest.Compare(t, fi[i].Mtime, uint64(stats[i].ModTime().UnixNano()))
		ifstest.Compare(t, fi[i].IsDir, false)
	}

	err = os.RemoveAll(cachePath)
	ifstest.Ok(t, err)

	ifstest.RemoveTempFile("test2/file1")
	ifstest.RemoveTempFile("test2/file2")
	os.Remove("/tmp/test2")

	dcm.Sender.Disconnect()
	foe.Receiver.Unbind()
}