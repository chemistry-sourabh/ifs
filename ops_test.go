// +build unit

package ifs

import (
	"testing"
	"github.com/google/go-cmp/cmp"
)

// TODO Check for specific errors
func TestAttr(t *testing.T) {

	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	payload := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := CreatePacket(AttrRequest, payload)

	s, err := Attr(pkt)

	if err != nil {
		t.Error("Got Error for Attr", err)
	}

	if s.Name != "file1" || s.Size != 0 || s.IsDir {
		t.Error("Got Wrong Stats", s)
	}

}

func TestAttr2(t *testing.T) {
	payload := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := CreatePacket(AttrRequest, payload)

	s, err := Attr(pkt)

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

	payload := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/dir1",
	}

	pkt := CreatePacket(ReadDirRequest, payload)

	stats, err := ReadDir(pkt)

	if err != nil {
		t.Error("Got Error in ReadDir", err)
	}

	arr := stats.Stats

	if len(arr) != 2 {
		t.Error("Unknown Files returned", arr)
	}

	// TODO Check File names

}

func TestReadDir2(t *testing.T) {
	payload := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/dir1",
	}

	pkt := CreatePacket(ReadDirRequest, payload)

	stats, err := ReadDir(pkt)

	if err == nil {
		t.Error("err is nil")
	}

	if stats != nil {
		t.Error("stats are not nil")
	}
}

func TestFetchFile(t *testing.T) {

	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	data := WriteDummyData("file1", 1000)

	payload := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := CreatePacket(FetchFileRequest, payload)

	chunk, err := FetchFile(pkt)

	chunk.Decompress()

	if err != nil {
		t.Error("Got Error in FetchFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data) {
		PrintTestError(t, "data fetched mismatch", chunk.Chunk, data)
	}

}

func TestFetchFile2(t *testing.T) {

	payload := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := CreatePacket(FetchFileRequest, payload)

	chunk, err := FetchFile(pkt)

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

	rp := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	payload := &ReadInfo{
		RemotePath: rp,
		Offset:     0,
		Size:       100,
	}

	pkt := CreatePacket(ReadFileRequest, payload)

	chunk, err := ReadFile(pkt)

	chunk.Decompress()

	if err != nil {
		t.Error("Got Error in ReadFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data[:100]) {
		PrintTestError(t, "chunks dont match", chunk.Chunk, data[:100])
	}

}

func TestReadFile2(t *testing.T) {
	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	data := WriteDummyData("file1", 1000)

	rp := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	payload := &ReadInfo{
		RemotePath: rp,
		Offset:     100,
		Size:       100,
	}

	pkt := CreatePacket(ReadFileRequest, payload)

	chunk, err := ReadFile(pkt)

	chunk.Decompress()

	if err != nil {
		t.Error("Got Error in ReadFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data[100:200]) {
		PrintTestError(t, "chunks dont match", chunk.Chunk, data[100:200])
	}
}

func TestReadFile3(t *testing.T) {
	CreateTempFile("file1")
	defer RemoveTempFile("file1")

	WriteDummyData("file1", 1000)

	rp := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	payload := &ReadInfo{
		RemotePath: rp,
		Offset:     999,
		Size:       100,
	}

	pkt := CreatePacket(ReadFileRequest, payload)

	chunk, err := ReadFile(pkt)

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

	rp := &RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	payload := &ReadInfo{
		RemotePath: rp,
		Offset:     0,
		Size:       1000,
	}

	pkt := CreatePacket(ReadFileRequest, payload)

	chunk, err := ReadFile(pkt)

	chunk.Decompress()

	if err != nil {
		t.Error("Got Error in ReadFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data) {
		PrintTestError(t, "chunks dont match", chunk.Chunk, data)
	}
}
