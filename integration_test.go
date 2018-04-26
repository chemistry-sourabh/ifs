// +build integration

package ifs

import (
	"testing"
	"os"
	"time"
	"path"
	"strconv"
	"syscall"
	"io/ioutil"
)

const TestRoot = "/tmp/test_root"
const TestCache = "/tmp/test_cache"
const TestRemoteRoot = "/tmp/test_remote_root"

const SmallFileCount = 10

func StartAgentProcess() {
	go func() {
		StartAgent("0.0.0.0", 8000)
	}()
}

func StartFsProcess(cfg *FsConfig) {
	go func() {
		MountRemoteRoots(cfg)
	}()
}

func CreateConfig() *FsConfig {
	return &FsConfig{
		MountPoint:    TestRoot,
		CacheLocation: TestCache,
		RemoteRoot: &RemoteRoot{
			Address: "localhost:8000",
			Paths:   []string{TestRemoteRoot},
		},
		Log: &LogConfig{
			Logging: false,
		},

		ConnCount: 3,
	}
}

func CreateTestDirs() {
	os.MkdirAll(TestRoot, 0755)
	os.MkdirAll(TestCache, 0755)
	os.MkdirAll(TestRemoteRoot, 0755)
}

func DeleteTestDirs() {
	os.Remove(TestRoot)
	os.Remove(TestCache)
	os.Remove(TestRemoteRoot)
}

func CreateTestFile(name string) error {
	fullPath := path.Join(TestRoot, "test_remote_root", name)
	f, err := os.Create(fullPath)
	if err == nil {
		f.Close()
	}

	return err
}

func CreateTestFileRemote(name string) {
	fullPath := path.Join(TestRemoteRoot, name)
	f, err := os.Create(fullPath)
	if err == nil {
		f.Close()
	}
}

func CreateTestDir(name string) error {
	fullPath := path.Join(TestRoot, "test_remote_root", name)
	return os.Mkdir(fullPath, 0755)
}

func RemoveTestFile(name string) error {
	fullPath := path.Join(TestRoot, "test_remote_root", name)
	return os.Remove(fullPath)
}

func RemoveTestFileRemote(name string) {
	fullPath := path.Join(TestRemoteRoot, name)
	os.Remove(fullPath)
}

func GetFileName(i int) string {
	return "file" + strconv.FormatInt(int64(i), 10)
}

func GetDirName(i int) string {
	return "dir" + strconv.FormatInt(int64(i), 10)
}

func Setup() {
	CreateTestDirs()
	cfg := CreateConfig()
	SetupLogger(cfg.Log)
	StartAgentProcess()
	time.Sleep(1 * time.Second)
	StartFsProcess(cfg)
	time.Sleep(1 * time.Second)
}

func Teardown() {
	time.Sleep(1 * time.Second)
	syscall.Unmount(TestRoot, 1) // 1 is MNT_FORCE
	DeleteTestDirs()
}

func ContainsInArray(arr []string, str string) bool {
	for _, v := range arr {
		if v == str {
			return true
		}
	}
	return false
}

func TestCreateAndRemove(t *testing.T) {

	for i := 0; i < SmallFileCount; i++ {
		err := CreateTestFile(GetFileName(i))
		IsError(t,"Create file failed "+GetFileName(i), err)
	}

	for i := 0; i < SmallFileCount; i++ {
		err := RemoveTestFile(GetFileName(i))
		IsError(t, "Remove file failed "+GetFileName(i), err)
	}

}

func TestMkdirAndRemove(t *testing.T) {

	for i := 0; i < SmallFileCount; i++ {
		err := CreateTestDir(GetDirName(i))
		IsError(t,"Create file failed "+GetDirName(i), err)
	}

	for i := 0; i < SmallFileCount; i++ {
		err := RemoveTestFile(GetDirName(i))
		IsError(t, "Remove file failed "+GetDirName(i), err)
	}

}

func TestReadDirAll(t *testing.T) {
	for i := 0; i < SmallFileCount; i++ {
		CreateTestFileRemote(GetFileName(i))
	}

	fullPath := path.Join(TestRoot, "test_remote_root")
	files, err := ioutil.ReadDir(fullPath)

	IsError(t, "ReadDir gave error", err)

	Compare(t, "File Count", len(files), SmallFileCount)

	var names []string
	for _, file := range files {
		names = append(names, file.Name())
	}

	var actNames []string
	for i := 0; i< SmallFileCount; i++ {
		actNames = append(actNames, GetFileName(i))
	}

	for i := 0; i < SmallFileCount; i++ {
		if !ContainsInArray(names, GetFileName(i))  {
			PrintTestError(t, "Dir content is wrong", names, actNames)
		}
	}

	for i := 0; i < SmallFileCount; i++ {
		RemoveTestFileRemote(GetFileName(i))
	}
}


func TestSetAttr(t *testing.T) {
	CreateTestFileRemote(GetFileName(0))
	defer RemoveTestFile(GetFileName(0))

	fullPath := path.Join(TestRemoteRoot, GetFileName(0))
	WriteDummyData(fullPath, 100)

	err := os.Truncate(fullPath, 10)

	IsError(t, "Got error in setattr", err)

	f, _ := os.Lstat(fullPath)

	Compare(t, "size", int(f.Size()), 10)
}


func TestMain(m *testing.M) {
	Setup()
	m.Run()
	Teardown()
}
