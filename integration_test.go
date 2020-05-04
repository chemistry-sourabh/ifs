/*
Copyright 2018 Sourabh Bollapragada

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ifs_test

import (
	"fmt"
	"github.com/chemistry-sourabh/ifs"
	"github.com/chemistry-sourabh/ifs/ifstest"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"
	"syscall"
	"testing"
	"time"
)


// TODO Check Tree
const TestRoot = "/tmp/test_root"
const TestCache = "/tmp/test_cache"
const TestRemoteRoot = "/tmp/test_remote_root"

const SmallFileCount = 10
const FileNameLimit = 1000

func StartAgentProcess() {
	go func() {
		ifs.StartAgent("localhost", 8000)
	}()
}

func StartFsProcess(cfg *ifs.FsConfig) {
	go func() {
		ifs.MountRemoteRoots(cfg)
	}()
}

func CreateConfig() *ifs.FsConfig {
	return &ifs.FsConfig{
		MountPoint: TestRoot,
		CachePath:  TestCache,
		RemoteRoots: []*ifs.RemoteRoot{
			&ifs.RemoteRoot{
				Hostname: "localhost",
				Port:     8000,
				Paths:    []string{TestRemoteRoot},
			},
		},
		Log: &ifs.LogConfig{
			Logging: false,
			Console: true,
			Debug:   true,
		},
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
	fullPath := path.Join(TestRoot, "localhost", TestRemoteRoot, name)
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

func GetTestFilePath(i int) string {
	fname := GetFileName(i)
	return path.Join(TestRoot, "localhost", TestRemoteRoot, fname)
}

func GetTestFileRemotePath(i int) string {
	fname := GetFileName(i)
	return path.Join(TestRemoteRoot, fname)
}

func CreateTestDir(name string) error {
	fullPath := path.Join(TestRoot, "localhost", TestRemoteRoot, name)
	return os.Mkdir(fullPath, 0755)
}

func CreateTestDirRemote(name string) error {
	fullPath := path.Join(TestRemoteRoot, name)
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

func TestCreate(t *testing.T) {
	start := rand.Intn(FileNameLimit)
	var files []string
	for i := start; i < start+SmallFileCount; i++ {
		err := CreateTestFile(GetFileName(i))
		ifstest.Ok(t, err)
		files = append(files, GetFileName(i))
	}

	stats, _ := ioutil.ReadDir(TestRemoteRoot)

	ifstest.Compare(t, len(stats), SmallFileCount)
	for _, stat := range stats {
		ifstest.Compare(t, ContainsInArray(files, stat.Name()), true)
	}

	for i := start; i < start+SmallFileCount; i++ {
		RemoveTestFileRemote(GetFileName(i))
	}
}

func TestRemove(t *testing.T) {
	start := rand.Intn(FileNameLimit)
	for i := start; i < start+SmallFileCount; i++ {
		CreateTestFileRemote(GetFileName(i))
	}

	for i := start; i < start+SmallFileCount; i++ {
		err := RemoveTestFile(GetFileName(i))
		ifstest.Ok(t, err)
	}
}

func TestMkdir(t *testing.T) {
	start := rand.Intn(FileNameLimit)
	var dirs []string
	for i := start; i < start+SmallFileCount; i++ {
		err := CreateTestDir(GetDirName(i))
		ifstest.Ok(t, err)
		dirs = append(dirs, GetDirName(i))
	}

	stats, _ := ioutil.ReadDir(TestRemoteRoot)
	ifstest.Compare(t, len(stats), SmallFileCount)
	for _, stat := range stats {
		ifstest.Compare(t, ContainsInArray(dirs, stat.Name()), true)
	}

	for i := start; i < start+SmallFileCount; i++ {
		RemoveTestFileRemote(GetDirName(i))
	}

}

func TestRemoveDir(t *testing.T) {
	start := rand.Intn(FileNameLimit)
	for i := start; i < start+SmallFileCount; i++ {
		CreateTestDirRemote(GetDirName(i))
	}

	for i := start; i < start+SmallFileCount; i++ {
		err := RemoveTestFile(GetDirName(i))
		ifstest.Ok(t, err)
	}

}

func TestReadDirAll(t *testing.T) {
	start := rand.Intn(FileNameLimit)
	for i := start; i < start+SmallFileCount; i++ {
		CreateTestFileRemote(GetFileName(i))
	}

	fullPath := path.Join(TestRoot, "localhost", TestRemoteRoot)
	files, err := ioutil.ReadDir(fullPath)

	// No Error After Read
	ifstest.Ok(t, err)

	// checking number of files
	ifstest.Compare(t, len(files), SmallFileCount)

	var names []string
	for _, file := range files {
		names = append(names, file.Name())
	}

	var actNames []string
	for i := start; i < start+SmallFileCount; i++ {
		actNames = append(actNames, GetFileName(i))
	}

	for i := start; i < start+SmallFileCount; i++ {
		if !ContainsInArray(names, GetFileName(i)) {
			ifstest.PrintTestError(t, "Flags content is wrong", names, actNames)
		}
	}

	for i := start; i < start+SmallFileCount; i++ {
		RemoveTestFileRemote(GetFileName(i))
	}
}

func TestSetAttrSize(t *testing.T) {
	fname := rand.Intn(FileNameLimit)
	CreateTestFileRemote(GetFileName(fname))
	defer RemoveTestFileRemote(GetFileName(fname))

	rp := GetTestFileRemotePath(fname)
	localPath := GetTestFilePath(fname)

	ifstest.WriteDummyData(rp, 100)

	err := os.Truncate(localPath, 10)
	ifstest.Ok(t, err)

	f, _ := os.Lstat(rp)
	ifstest.Compare(t, int(f.Size()), 10)

	f, _ = os.Lstat(localPath)
	ifstest.Compare(t, int(f.Size()), 10)
}

func TestSetAttrMode(t *testing.T) {
	fname := rand.Intn(FileNameLimit)
	CreateTestFileRemote(GetFileName(fname))
	defer RemoveTestFileRemote(GetFileName(fname))

	rp := GetTestFileRemotePath(fname)
	localPath := GetTestFilePath(fname)

	f, _ := os.Lstat(rp)
	ifstest.Compare(t, f.Mode(), os.FileMode(ifstest.DefaultPerm()))

	f, _ = os.Lstat(localPath)
	ifstest.Compare(t, f.Mode(), os.FileMode(ifstest.DefaultPerm()))

	os.Chmod(localPath, 0666)

	f, _ = os.Lstat(rp)
	ifstest.Compare(t, f.Mode(), os.FileMode(0666))

	f, _ = os.Lstat(localPath)
	ifstest.Compare(t, f.Mode(), os.FileMode(0666))
}

func TestSetAttrTime(t *testing.T) {
	fname := rand.Intn(FileNameLimit)
	CreateTestFileRemote(GetFileName(fname))
	defer RemoveTestFileRemote(GetFileName(fname))

	rp := GetTestFileRemotePath(fname)
	localPath := GetTestFilePath(fname)

	now := time.Now().Add(10 * time.Minute)
	os.Chtimes(localPath, now, now)

	f, _ := os.Lstat(rp)
	ifstest.Compare(t, f.ModTime().Unix(), now.Unix())

	f, _ = os.Lstat(localPath)
	ifstest.Compare(t, f.ModTime().Unix(), now.Unix())

}

func TestOpenClose(t *testing.T) {
	fname := rand.Intn(FileNameLimit)
	CreateTestFileRemote(GetFileName(fname))
	defer RemoveTestFileRemote(GetFileName(fname))

	localPath := GetTestFilePath(fname)

	f, err := os.OpenFile(localPath, os.O_RDWR, 0666)
	ifstest.Ok(t, err)

	err = f.Close()
	ifstest.Ok(t, err)
}

func TestWrite(t *testing.T) {
	fname := rand.Intn(FileNameLimit)
	CreateTestFileRemote(GetFileName(fname))
	defer RemoveTestFileRemote(GetFileName(fname))

	localPath := GetTestFilePath(fname)
	rp := GetTestFileRemotePath(fname)

	data := ifstest.WriteDummyDataToPath(localPath, 100)

	f, _ := os.Stat(rp)
	ifstest.Compare(t, f.Size(), int64(100))

	f, _ = os.Stat(localPath)
	ifstest.Compare(t, f.Size(), int64(100))

	read, _ := ioutil.ReadFile(rp)
	ifstest.Compare(t, read, data)
}

func TestRead(t *testing.T) {
	fname := rand.Intn(FileNameLimit)
	CreateTestFileRemote(GetFileName(fname))
	defer RemoveTestFileRemote(GetFileName(fname))

	rp := GetTestFileRemotePath(fname)
	localPath := GetTestFilePath(fname)
	data := ifstest.WriteDummyDataToPath(rp, 100)

	ioutil.ReadDir(path.Join(TestRoot, "localhost", TestRemoteRoot))
	read, _ := ioutil.ReadFile(localPath)
	ifstest.Compare(t, len(read), 100)
	ifstest.Compare(t, read, data)
}

func TestRename(t *testing.T) {
	fname := rand.Intn(FileNameLimit)
	CreateTestFileRemote(GetFileName(fname))

	rp := GetTestFileRemotePath(fname)
	newRp := GetTestFileRemotePath(fname + 1)
	localPath := GetTestFilePath(fname)
	newPath := GetTestFilePath(fname + 1)

	_, err := os.Lstat(rp)
	ifstest.Ok(t, err)

	_, err = os.Lstat(newRp)
	ifstest.Err(t, err)

	err = os.Rename(localPath, newPath)
	ifstest.Ok(t, err)

	_, err = os.Lstat(localPath)
	ifstest.Err(t, err)

	_, err = os.Lstat(newPath)
	ifstest.Ok(t, err)

	_, err = os.Lstat(rp)
	ifstest.Err(t, err)

	_, err = os.Lstat(newRp)
	ifstest.Ok(t, err)

	RemoveTestFileRemote(GetFileName(fname + 1))
}

func TestAttrSync(t *testing.T) {
	fname := rand.Intn(FileNameLimit)
	CreateTestFileRemote(GetFileName(fname))
	defer RemoveTestFileRemote(GetFileName(fname))

	f, _ := os.Lstat(GetTestFilePath(fname))
	ifstest.Compare(t, f.Mode(), os.FileMode(ifstest.DefaultPerm()))

	os.Chmod(GetTestFileRemotePath(fname), 0666)

	f, _ = os.Lstat(GetTestFileRemotePath(fname))
	ifstest.Compare(t, f.Mode(), os.FileMode(0666))

	ioutil.ReadDir(path.Join(TestRoot, "localhost", TestRemoteRoot))
	f, _ = os.Lstat(GetTestFilePath(fname))
	ifstest.Compare(t, f.Mode(), os.FileMode(0666))
}

// TODO Sync Tests
// TODO Multiple Mounts
// TODO Error Tests
// TODO Cache Tests
// TODO Multithreaded Test
// TODO In Directory Tests

func TestMain(m *testing.M) {
	seed := time.Now().UnixNano()
	fmt.Printf("Seed is %d\n", seed)
	rand.Seed(seed)
	Setup()
	exitCode := m.Run()
	Teardown()
	os.Exit(exitCode)
}
