// +build unit

package unit

import (
	"testing"
	"github.com/google/go-cmp/cmp"
	"ifs"
	"ifs/test"
)

// TODO Check for specific errors
func TestAttr(t *testing.T) {

	test.CreateTempFile("file1")
	defer test.RemoveTempFile("file1")

	payload := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := test.CreatePacket(ifs.AttrRequest, payload)

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

	pkt := test.CreatePacket(ifs.AttrRequest, payload)

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

	test.CreateTempDir("dir1")
	defer test.RemoveTempFile("dir1")

	test.CreateTempFile("dir1/file1")
	defer test.RemoveTempFile("dir1/file1")

	test.CreateTempFile("dir1/file2")
	defer test.RemoveTempFile("dir1/file2")

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

	pkt := test.CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadDirInfo{
		Path:           remotePath.Path,
		FileDescriptor: 1,
	}

	pkt = test.CreatePacket(ifs.ReadDirRequest, payload1)

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

	pkt = test.CreatePacket(ifs.CloseRequest, payload2)

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

	pkt := test.CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadDirInfo{
		Path:           remotePath.Path,
		FileDescriptor: 1,
	}

	pkt = test.CreatePacket(ifs.ReadDirRequest, payload1)

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

	pkt = test.CreatePacket(ifs.CloseRequest, payload2)

	fh.CloseFile(pkt)
}

func TestFetchFile(t *testing.T) {

	test.CreateTempFile("file1")
	defer test.RemoveTempFile("file1")

	data := test.WriteDummyData("file1", 1000)

	payload := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := test.CreatePacket(ifs.FetchFileRequest, payload)

	fh := ifs.AgentFileHandler()
	chunk, err := fh.FetchFile(pkt)

	chunk.Decompress()

	if err != nil {
		t.Error("Got Error in FetchFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data) {
		test.PrintTestError(t, "data fetched mismatch", chunk.Chunk, data)
	}

}

func TestFetchFile2(t *testing.T) {

	payload := &ifs.RemotePath{
		Hostname: "localhost",
		Port:     11211,
		Path:     "/tmp/file1",
	}

	pkt := test.CreatePacket(ifs.FetchFileRequest, payload)

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

	test.CreateTempFile("file1")
	defer test.RemoveTempFile("file1")

	data := test.WriteDummyData("file1", 1000)

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

	pkt := test.CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Offset:         0,
		Size:           100,
	}

	pkt = test.CreatePacket(ifs.ReadFileRequest, payload1)

	chunk, err := fh.ReadFile(pkt)

	chunk.Decompress()

	if err != nil {
		t.Error("Got Error in ReadFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data[:100]) {
		test.PrintTestError(t, "chunks dont match", chunk.Chunk, data[:100])
	}

	payload2 := &ifs.CloseInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
	}

	pkt = test.CreatePacket(ifs.CloseRequest, payload2)

	fh.CloseFile(pkt)

}

func TestReadFile2(t *testing.T) {
	test.CreateTempFile("file1")
	defer test.RemoveTempFile("file1")

	data := test.WriteDummyData("file1", 1000)

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

	pkt := test.CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Offset:         100,
		Size:           100,
	}

	pkt = test.CreatePacket(ifs.ReadFileRequest, payload1)

	chunk, err := fh.ReadFile(pkt)

	chunk.Decompress()

	if err != nil {
		t.Error("Got Error in ReadFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data[100:200]) {
		test.PrintTestError(t, "chunks dont match", chunk.Chunk, data[100:200])
	}

	payload2 := &ifs.CloseInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
	}

	pkt = test.CreatePacket(ifs.CloseRequest, payload2)

	fh.CloseFile(pkt)
}

func TestReadFile3(t *testing.T) {
	test.CreateTempFile("file1")
	defer test.RemoveTempFile("file1")

	test.WriteDummyData("file1", 1000)

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

	pkt := test.CreatePacket(ifs.ReadFileRequest, payload)

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
	test.CreateTempFile("file1")
	defer test.RemoveTempFile("file1")

	data := test.WriteDummyData("file1", 1000)

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

	pkt := test.CreatePacket(ifs.OpenRequest, payload)

	fh.OpenFile(pkt)

	payload1 := &ifs.ReadInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
		Offset:         0,
		Size:           1000,
	}

	pkt = test.CreatePacket(ifs.ReadFileRequest, payload1)

	chunk, err := fh.ReadFile(pkt)

	chunk.Decompress()

	if err != nil {
		t.Error("Got Error in ReadFile", err)
	}

	if !cmp.Equal(chunk.Chunk, data) {
		test.PrintTestError(t, "chunks dont match", chunk.Chunk, data)
	}

	payload2 := &ifs.CloseInfo{
		Path:           rp.Path,
		FileDescriptor: 1,
	}

	pkt = test.CreatePacket(ifs.CloseRequest, payload2)

	fh.CloseFile(pkt)
}
