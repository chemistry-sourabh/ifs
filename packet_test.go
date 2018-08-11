// +build unit

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
	"testing"
	"github.com/google/go-cmp/cmp"
	"github.com/vmihailenco/msgpack"
	"io"
	"ifs"
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

	pkt := CreatePacket(ifs.ReadFileRequest, payload)

	data, err := pkt.Marshal()

	// No Error
	Ok(t, err)
	// Should not be nil
	NotNil(t, data)

	header := data[:11]

	// Compare op code
	Compare(t, header[8], uint8(ifs.ReadFileRequest))

	id := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	// Compare Request Id
	Compare(t, header[:8], id)

	rp := &ifs.RemotePath{}

	msgpack.Unmarshal(data[11:], rp)

	// Compare Payload
	Compare(t, rp, payload)

}

func TestPacket_Marshal2(t *testing.T) {
	pkt := CreatePacket(ifs.ReadFileRequest, nil)

	data, err := pkt.Marshal()

	if err != nil {
		t.Error("Got error in Marshal", err)
	}

	if data == nil {
		t.Error("data is nil")
	}

	header := data[:11]

	if header[8] != ifs.ReadFileRequest {
		PrintTestError(t, "op code not matching", header[8], ifs.ReadFileRequest)
	}

	id := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	// Compare Request Id
	Compare(t, header[:8], id)

	var n interface{}

	msgpack.Unmarshal(data[11:], &n)

	if n != nil {
		PrintTestError(t, "data is not nil", n, nil)
	}

}

func TestPacket_Marshal3(t *testing.T) {
	payload := &ifs.Error{
		Err: io.EOF,
	}

	pkt := CreatePacket(ifs.FileDataResponse, payload)

	data, err := pkt.Marshal()

	if err != nil {
		t.Error("Got error in Marshal", err)
	}

	if data == nil {
		t.Error("data is nil")
	}

	header := data[:11]

	if header[8] != ifs.FileDataResponse {
		PrintTestError(t, "op code not matching", header[8], ifs.FileDataResponse)
	}

	id := []byte{0, 0, 0, 0, 0, 0, 0, 0}

	if !cmp.Equal(header[:8], id) {
		PrintTestError(t, "request Id doesnt match", header[:8], id)
	}

	e := ifs.Error{}
	msgpack.Unmarshal(data[11:], &e)

	if !cmp.Equal(e.Err.Error(), io.EOF.Error()) {
		PrintTestError(t, "errors dont match", e.Err, io.EOF)
	}

}

// Marshalling Fails Dont know if this possible
//func TestPacket_Marshal4(t *testing.T) {
//	t.Skip()
//}
