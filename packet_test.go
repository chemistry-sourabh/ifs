package ifs

import (
	"testing"
	"github.com/google/go-cmp/cmp"
	"github.com/vmihailenco/msgpack"
	"io"
)

//TODO Check Proper Id and ConnId
//TODO Remove Verbose Log Printing
func TestPacket_Marshal(t *testing.T) {

	payload := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := CreatePacket(ReadFileRequest, payload)

	data, err := pkt.Marshal()

	IsError(t, "Got error in Marshal", err)
	IsNil(t, "data", data)

	header := data[:10]

	Compare(t, "op code", header[8], uint8(ReadFileRequest))

	id := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	Compare(t, "request id", header[:8], id)

	rp := &RemotePath{}

	msgpack.Unmarshal(data[10:], rp)

	Compare(t, "payload", rp, payload)

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

	header := data[:10]

	if header[8] != ReadFileRequest {
		PrintTestError(t, "op code not matching", header[8], ReadFileRequest)
	}

	id := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	Compare(t, "request id", header[:8], id)

	var n interface{}

	msgpack.Unmarshal(data[10:], &n)

	if n != nil {
		PrintTestError(t, "data is not nil", n, nil)
	}

}

func TestPacket_Marshal3(t *testing.T) {
	payload := &Error{
		Err: io.EOF,
	}

	pkt := CreatePacket(FileDataResponse, payload)

	data, err := pkt.Marshal()

	if err != nil {
		t.Error("Got error in Marshal", err)
	}

	if data == nil {
		t.Error("data is nil")
	}

	header := data[:10]

	if header[8] != FileDataResponse {
		PrintTestError(t, "op code not matching", header[8], FileDataResponse)
	}

	id := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	if !cmp.Equal(header[:8], id) {
		PrintTestError(t, "request Id doesnt match", header[:8], id)
	}

	e := Error{}
	msgpack.Unmarshal(data[10:], &e)

	if !cmp.Equal(e.Err.Error(), io.EOF.Error()) {
		PrintTestError(t, "errors dont match", e.Err, io.EOF)
	}

}

// Marshalling Fails Dont know if this possible
func TestPacket_Marshal4(t *testing.T) {
	t.Skip()
}
