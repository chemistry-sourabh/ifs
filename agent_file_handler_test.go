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
	"github.com/chemistry-sourabh/ifs"
)

// TODO Check for specific errors
func TestAttr(t *testing.T) {

	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	payload := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := CreatePacket(ifs.AttrRequest, payload)

	fh := ifs.AgentFileHandler()
	s, err := fh.Attr(pkt)

	if err != nil {
		t.Error("Got Error for Attr", err)
	}

	if s.Name != "file1" || s.Size != 0 || s.IsDir {
		t.Error("Got Wrong Stats", s)
	}

}

func TestAttr2(t *testing.T) {
	payload := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := CreatePacket(ifs.AttrRequest, payload)

	fh := ifs.AgentFileHandler()

	s, err := fh.Attr(pkt)

	if err == nil {
		t.Error("Didnt get error")
	}

	if s != nil {
		t.Error("Got Stats along with error")
	}
}

func TestReadDir(t *testing.T) {

	CreateTempDir("dir1")
	defer RemoveTempFile("dir1")

	CreateTempFile("dir1/file1")
	defer RemoveTempFile("dir1/file1")

	CreateTempFile("dir1/file2")
	defer RemoveTempFile("dir1/file2")

	remotePath := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/dir1",
	}

	fh := ifs.AgentFileHandler()

	payload := &ifs.OpenInfo{
		Path:           remotePath.Path,
		FileDescriptor: 1,
		Flags:          0,
	}

	pkt := CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadDirInfo{
		Path:           remotePath.Path,
		FileDescriptor: 1,
	}

	pkt = CreatePacket(ifs.ReadDirRequest, payload1)

	stats, err := fh.ReadDir(pkt)

	if err != nil {
		t.Error("Got Error in ReadDir", err)
	}

	arr := stats.Stats

	if len(arr) != 2 {
		t.Error("Unknown Files returned", arr)
	}

	// TODO Check File names

	payload2 := &ifs.CloseInfo{
		Path:           remotePath.Path,
		FileDescriptor: 1,
	}

	pkt = CreatePacket(ifs.CloseRequest, payload2)

	fh.CloseFile(pkt)

}

func TestReadDir2(t *testing.T) {
	remotePath := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/dir1",
	}

	fh := ifs.AgentFileHandler()

	payload := &ifs.OpenInfo{
		Path:           remotePath.Path,
		FileDescriptor: 1,
		Flags:          0,
	}

	pkt := CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadDirInfo{
		Path:           remotePath.Path,
		FileDescriptor: 1,
	}

	pkt = CreatePacket(ifs.ReadDirRequest, payload1)

	stats, err := fh.ReadDir(pkt)

	if err == nil {
		t.Error("err is nil")
	}

	if stats != nil {
		t.Error("stats are not nil")
	}

	payload2 := &ifs.CloseInfo{
		Path:           remotePath.Path,
		FileDescriptor: 1,
	}

	pkt = CreatePacket(ifs.CloseRequest, payload2)

	fh.CloseFile(pkt)
}

func TestFetchFile(t *testing.T) {

	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	data := WriteDummyData("file1", 1000)

	payload := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := CreatePacket(ifs.FetchFileRequest, payload)

	fh := ifs.AgentFileHandler()
	chunk, err := fh.FetchFile(pkt)

	//chunk.Decompress()

	if err != nil {
		t.Error("Got Error in FetchFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data) {
		PrintTestError(t, "data fetched mismatch", chunk.Chunk, data)
	}

}

func TestFetchFile2(t *testing.T) {

	payload := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := CreatePacket(ifs.FetchFileRequest, payload)

	fh := ifs.AgentFileHandler()
	chunk, err := fh.FetchFile(pkt)

	if err == nil {
		t.Error("err is nil")
	}

	if chunk != nil {
		t.Error("chunk are not nil")
	}
}

func TestReadFile(t *testing.T) {

	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	data := WriteDummyData("file1", 1000)

	rp := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	fh := ifs.AgentFileHandler()

	payload := &ifs.OpenInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Flags:          0,
	}

	pkt := CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Offset:         0,
		Size:           100,
	}

	pkt = CreatePacket(ifs.ReadFileRequest, payload1)

	chunk, err := fh.ReadFile(pkt)

	//chunk.Decompress()

	if err != nil {
		t.Error("Got Error in ReadFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data[:100]) {
		PrintTestError(t, "chunks dont match", chunk.Chunk, data[:100])
	}

	payload2 := &ifs.CloseInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
	}

	pkt = CreatePacket(ifs.CloseRequest, payload2)

	fh.CloseFile(pkt)

}

func TestReadFile2(t *testing.T) {
	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	data := WriteDummyData("file1", 1000)

	rp := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	fh := ifs.AgentFileHandler()

	payload := &ifs.OpenInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Flags:          0,
	}

	pkt := CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Offset:         100,
		Size:           100,
	}

	pkt = CreatePacket(ifs.ReadFileRequest, payload1)

	chunk, err := fh.ReadFile(pkt)

	//chunk.Decompress()

	if err != nil {
		t.Error("Got Error in ReadFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data[100:200]) {
		PrintTestError(t, "chunks dont match", chunk.Chunk, data[100:200])
	}

	payload2 := &ifs.CloseInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
	}

	pkt = CreatePacket(ifs.CloseRequest, payload2)

	fh.CloseFile(pkt)
}

func TestReadFile3(t *testing.T) {
	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	WriteDummyData("file1", 1000)

	rp := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	payload := &ifs.ReadInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Offset:         999,
		Size:           100,
	}

	pkt := CreatePacket(ifs.ReadFileRequest, payload)

	fh := ifs.AgentFileHandler()
	chunk, err := fh.ReadFile(pkt)

	// EOF Error
	if err == nil {
		t.Error("err is nil")
	}

	if chunk != nil {
		t.Error("chunk is not nil")
	}
}

func TestReadFile4(t *testing.T) {
	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	data := WriteDummyData("file1", 1000)

	rp := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	fh := ifs.AgentFileHandler()

	payload := &ifs.OpenInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Flags:          0,
	}

	pkt := CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Offset:         0,
		Size:           1000,
	}

	pkt = CreatePacket(ifs.ReadFileRequest, payload1)

	chunk, err := fh.ReadFile(pkt)

	//chunk.Decompress()

	if err != nil {
		t.Error("Got Error in ReadFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data) {
		PrintTestError(t, "chunks dont match", chunk.Chunk, data)
	}

	payload2 := &ifs.CloseInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
	}

	pkt = CreatePacket(ifs.CloseRequest, payload2)

	fh.CloseFile(pkt)
}

func TestReadFile5(t *testing.T) {
	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	data := WriteDummyData("file1", 1000)

	rp := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	fh := ifs.AgentFileHandler()

	payload := &ifs.OpenInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Flags:          0,
	}

	pkt := CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Offset:         0,
		Size:           4096,
	}

	pkt = CreatePacket(ifs.ReadFileRequest, payload1)

	chunk, err := fh.ReadFile(pkt)

	//chunk.Decompress()

	Ok(t, err)

	Compare(t, chunk.Chunk, data)

	payload2 := &ifs.CloseInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
	}

	pkt = CreatePacket(ifs.CloseRequest, payload2)

	fh.CloseFile(pkt)
}
