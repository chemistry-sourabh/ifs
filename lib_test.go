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
	"crypto/rand"
	"encoding/binary"
	"github.com/chemistry-sourabh/ifs"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"testing"
)

func PrintTestError(t *testing.T, message string, got interface{}, want interface{}) {
	t.Errorf("%s, got: %s, want %s", message, got, want)
}

func CreateTempFile(name string) {
	fPath := path.Join("/tmp", name)
	f, _ := os.Create(fPath)
	defer f.Close()
}

func RemoveTempFile(name string) {
	fPath := path.Join("/tmp", name)
	os.Remove(fPath)
}

func CreateTempDir(name string) {
	fPath := path.Join("/tmp", name)
	os.MkdirAll(fPath, 0755)
}

func CreateTestConfig() {

}

func CreatePacket(opCode uint8, payload ifs.Payload) *ifs.Packet {
	return &ifs.Packet{
		Id:    0,
		Flags: 0,
		Op:    opCode,
		Data:  payload,
	}
}

func WriteDummyData(name string, size int) []byte {
	fPath := path.Join("/tmp", name)
	data := make([]byte, size)
	binary.Read(rand.Reader, binary.LittleEndian, &data)
	ioutil.WriteFile(fPath, data, 0666)
	return data
}

func WriteDummyDataToPath(fullPath string, size int) []byte {
	data := make([]byte, size)
	binary.Read(rand.Reader, binary.LittleEndian, &data)
	ioutil.WriteFile(fullPath, data, 0666)
	return data
}

func DefaultPerm() int {
	if runtime.GOOS == "darwin" {
		return 0644
	} else {
		return 0664
	}
}
