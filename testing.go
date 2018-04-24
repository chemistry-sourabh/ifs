package ifs

import (
	"testing"
	"os"
	"path"
	"io/ioutil"
	"encoding/binary"
	"crypto/rand"
	"github.com/google/go-cmp/cmp"
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


func CreatePacket(opCode uint8, payload Payload) *Packet {
	return &Packet{
		Id: 0,
		Op: opCode,
		Data: payload,
	}
}

func WriteDummyData(name string, size int) []byte{
	fPath := path.Join("/tmp", name)
	data := make([]byte, size)
	binary.Read(rand.Reader, binary.LittleEndian, &data)
	ioutil.WriteFile(fPath, data, 0666)
	return data
}

func Compare(t *testing.T, msg string, got interface{}, want interface{}) {
	if !cmp.Equal(got, want) {
		PrintTestError(t, msg+" don't match", got, want)
	}
}

func IsError(t *testing.T, msg string, err error) {
	if err != nil {
		t.Error(msg, err)
	}
}

func IsNil(t *testing.T, msg string, val interface{}) {
	if val == nil {
		t.Error(msg+" is nil")
	}
}