package ifs

import (
	"testing"
	"os"
	"time"
	"path"
	"strconv"
	"syscall"
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

func StartFsProcess(cfg *Config) {
	go func() {
		MountRemoteRoots(cfg)
	}()
}

func CreateConfig() *Config {
	return &Config{
		MountPoint:    TestRoot,
		CacheLocation: TestCache,
		RemoteRoot: &RemoteRoot{
			Address: "localhost:8000",
			Paths:   []string{TestRemoteRoot},
		},
		Log: &LogConfig{
			Logging: true,
			Console: true,
			Debug:   true,
		},

		ConnCount: 3,
	}
}

func CreateTestDirs() {
	os.Mkdir(TestRoot, 0755)
	os.Mkdir(TestCache, 0755)
	os.Mkdir(TestRemoteRoot, 0755)
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

func CreateTestDir(name string) error {
	fullPath := path.Join(TestRoot, "test_remote_root", name)
	return os.Mkdir(fullPath, 0755)
}

func RemoveTestFile(name string) error {
	fullPath := path.Join(TestRoot, "test_remote_root", name)
	return os.Remove(fullPath)
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

func TestCreateAndRemove(t *testing.T) {
	Setup()

	for i := 0; i < 10; i++ {
		err := CreateTestFile(GetFileName(i))
		IsError(t,"Create file failed "+GetFileName(i), err)
	}

	for i := 0; i < 10; i++ {
		err := RemoveTestFile(GetFileName(i))
		IsError(t, "Remove file failed "+GetFileName(i), err)
	}

	Teardown()
}

func TestMkdirAndRemove(t *testing.T) {
	Setup()

	for i := 0; i < 10; i++ {
		err := CreateTestDir(GetDirName(i))
		IsError(t,"Create file failed "+GetDirName(i), err)
	}

	for i := 0; i < 10; i++ {
		err := RemoveTestFile(GetDirName(i))
		IsError(t, "Remove file failed "+GetDirName(i), err)
	}

	Teardown()
}
