package arsyncfs

import (
	"testing"
	"github.com/google/go-cmp/cmp"
	"github.com/vmihailenco/msgpack"
	"io"
	"fmt"
	"reflect"
)

func TestPacket_Marshal(t *testing.T) {

	payload := &RemotePath{
		Hostname:"localhost",
		Port: 11211,
		Path: "/tmp/file1",
	}

	pkt := CreatePacket(ReadFileRequest, payload)

	data, err := pkt.Marshal()

	if err != nil {
		t.Error("Got error in Marshal", err)
	}

	if data == nil {
		t.Error("data is nil")
	}

	header := data[:9]

	if header[8] != ReadFileRequest {
		PrintTestError(t, "op code not matching", header[8], ReadFileRequest)
	}

	id := []byte{0,0,0,0,0,0,0,0}

	if !cmp.Equal(header[:8], id) {
		PrintTestError(t, "request Id doesnt match", header[:8], id)
	}

	rp := &RemotePath{}

	msgpack.Unmarshal(data[9:], rp)

	if !cmp.Equal(payload, rp) {
		PrintTestError(t, "payload doesnt match", rp, payload)
	}

}

func TestPacket_Marshal2(t *testing.T) {
	pkt := CreatePacket(ReadFileRequest, nil)

	data, err := pkt.Marshal()

	if err != nil {
		t.Error("Got error in Marshal", err)
	}

	if data == nil {
		t.Error("data is nil")
	}

	header := data[:9]

	if header[8] != ReadFileRequest {
		PrintTestError(t, "op code not matching", header[8], ReadFileRequest)
	}

	id := []byte{0,0,0,0,0,0,0,0}

	if !cmp.Equal(header[:8], id) {
		PrintTestError(t, "request Id doesnt match", header[:8], id)
	}

	var n interface{}

	msgpack.Unmarshal(data[9:], &n)


	if n != nil {
		PrintTestError(t, "data is not nil", n, nil)
	}

}

type Error struct {
	Err error
}

func TestPacket_Marshal3(t *testing.T) {
	t.Skip()
	payload := &Error{
		Err:io.EOF,
	}

	pkt := CreatePacket(FileDataResponse, payload)

	data, err := pkt.Marshal()

	if err != nil {
		t.Error("Got error in Marshal", err)
	}

	if data == nil {
		t.Error("data is nil")
	}

	header := data[:9]

	if header[8] != FileDataResponse {
		PrintTestError(t, "op code not matching", header[8], FileDataResponse)
	}

	id := []byte{0,0,0,0,0,0,0,0}

	if !cmp.Equal(header[:8], id) {
		PrintTestError(t, "request Id doesnt match", header[:8], id)
	}

	e := Error{}
	msgpack.Unmarshal(data[9:], &e)

	fmt.Println(reflect.TypeOf(e.Err))
	fmt.Println(reflect.TypeOf(io.EOF))

	if !cmp.Equal(e.Err, io.EOF) {
		PrintTestError(t, "errors dont match", e.Err, io.EOF)
	}

	//fmt.Println(payload)
	//fmt.Println(json.Marshal(payload))
	//
	//var e interface{}
	//
	//msgpack.Unmarshal(data[9:], &e)
	//
	//fmt.Println(len(e.(map[string]interface{})))
}

// Packet is Error
// Marshalling Fails
