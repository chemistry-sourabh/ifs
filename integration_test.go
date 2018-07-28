// +build integration

package ifs_test

import (
	"testing"
	"os"
	"time"
	"path"
	"strconv"
	"syscall"
	"io/ioutil"
	"ifs"
)


const TestRoot = "/tmp/test_root"
const TestCache = "/tmp/test_cache"
const TestRemoteRoot = "/tmp/test_remote_root"

const SmallFileCount = 10

func StartAgentProcess() {
	go func() {
		ifs.StartAgent("0.0.0.0", 8000)
	}()
}

func StartFsProcess(cfg *ifs.FsConfig) {
	go func() {
		ifs.MountRemoteRoots(cfg)
	}()
}

func CreateConfig() *ifs.FsConfig {
	return &ifs.FsConfig{
		MountPoint:    TestRoot,
		CacheLocation: TestCache,
		RemoteRoots: []*ifs.RemoteRoot{
			&ifs.RemoteRoot{
				Hostname: "localhost",
				Port: 8000,
				Paths:   []string{TestRemoteRoot},
			},
		},
		Log: &ifs.LogConfig{
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
	fullPath := path.Join(TestRoot, "localhost", TestRemoteRoot , name)
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
	fullPath := path.Join(TestRoot, "localhost", TestRemoteRoot, name)
	return os.Mkdir(fullPath, 0755)
}

func RemoveTestFile(name string) error {
	fullPath := path.Join(TestRoot, "localhost", TestRemoteRoot, name)
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
	ifs.SetupLogger(cfg.Log)
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
		Ok(t, err)
	}

	for i := 0; i < SmallFileCount; i++ {
		err := RemoveTestFile(GetFileName(i))
		Ok(t, err)
	}

}

func TestMkdirAndRemove(t *testing.T) {

	for i := 0; i < SmallFileCount; i++ {
		err := CreateTestDir(GetDirName(i))
		Ok(t, err)
	}

	for i := 0; i < SmallFileCount; i++ {
		err := RemoveTestFile(GetDirName(i))
		Ok(t, err)
	}

}

func TestReadDirAll(t *testing.T) {
	for i := 0; i < SmallFileCount; i++ {
		CreateTestFileRemote(GetFileName(i))
	}

	fullPath := path.Join(TestRoot, "localhost", TestRemoteRoot)
	files, err := ioutil.ReadDir(fullPath)

	// No Error After Read
	Ok(t, err)

	// checking number of fikes
	Compare(t, len(files), SmallFileCount)

	var names []string
	for _, file := range files {
		names = append(names, file.Name())
	}

	var actNames []string
	for i := 0; i < SmallFileCount; i++ {
		actNames = append(actNames, GetFileName(i))
	}

	for i := 0; i < SmallFileCount; i++ {
		if !ContainsInArray(names, GetFileName(i)) {
			PrintTestError(t, "Flags content is wrong", names, actNames)
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

	// checking no error
	Ok(t, err)

	f, _ := os.Lstat(fullPath)

	// checking new size
	Compare(t, int(f.Size()), 10)
}

func TestMain(m *testing.M) {
	Setup()
	m.Run()
	Teardown()
}
