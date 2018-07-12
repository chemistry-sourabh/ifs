// +build unit

package unit

import (
	"testing"
	"github.com/google/go-cmp/cmp"
	"github.com/vmihailenco/msgpack"
	"io"
	"ifs"
	"ifs/test"
)

//TODO Check Proper Id and ConnId
//TODO Remove Verbose Log Printing
// TODO Check Flags
func TestPacket_Marshal(t *testing.T) {

	payload := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := test.CreatePacket(ifs.ReadFileRequest, payload)

	data, err := pkt.Marshal()

	// No Error
	test.Ok(t, err)
	// Should not be nil
	test.NotNil(t, data)

	header := data[:11]

	// Compare op code
	test.Compare(t, header[8], uint8(ifs.ReadFileRequest))

	id := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	// Compare Request Id
	test.Compare(t, header[:8], id)

	rp := &ifs.RemotePath{}

	msgpack.Unmarshal(data[11:], rp)

	// Compare Payload
	test.Compare(t, rp, payload)

}

func TestPacket_Marshal2(t *testing.T) {
	pkt := test.CreatePacket(ifs.ReadFileRequest, nil)

	data, err := pkt.Marshal()

	if err != nil {
		t.Error("Got error in Marshal", err)
	}

	if data == nil {
		t.Error("data is nil")
	}

	header := data[:11]

	if header[8] != ifs.ReadFileRequest {
		test.PrintTestError(t, "op code not matching", header[8], ifs.ReadFileRequest)
	}

	id := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	// Compare Request Id
	test.Compare(t, header[:8], id)

	var n interface{}

	msgpack.Unmarshal(data[11:], &n)

	if n != nil {
		test.PrintTestError(t, "data is not nil", n, nil)
	}

}

func TestPacket_Marshal3(t *testing.T) {
	payload := &ifs.Error{
		Err: io.EOF,
	}

	pkt := test.CreatePacket(ifs.FileDataResponse, payload)

	data, err := pkt.Marshal()

	if err != nil {
		t.Error("Got error in Marshal", err)
	}

	if data == nil {
		t.Error("data is nil")
	}

	header := data[:11]

	if header[8] != ifs.FileDataResponse {
		test.PrintTestError(t, "op code not matching", header[8], ifs.FileDataResponse)
	}

	id := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	if !cmp.Equal(header[:8], id) {
		test.PrintTestError(t, "request Id doesnt match", header[:8], id)
	}

	e := ifs.Error{}
	msgpack.Unmarshal(data[11:], &e)

	if !cmp.Equal(e.Err.Error(), io.EOF.Error()) {
		test.PrintTestError(t, "errors dont match", e.Err, io.EOF)
	}

}

// Marshalling Fails Dont know if this possible
//func TestPacket_Marshal4(t *testing.T) {
//	t.Skip()
//}
