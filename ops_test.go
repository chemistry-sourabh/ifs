package arsyncfs

import "testing"

func TestAttr_Success(t *testing.T) {

	CreateTempFile("file1")

	payload := &RemotePath{
		Hostname: "localhost",
		Port: 11211,
		Path: "/tmp/file1",
	}

	pkt := CreatePacket(AttrRequest, payload)

	s, err := Attr(pkt)

	if err != nil {
		t.Error("Got Error for Attr", err)
	}

	if s.Name != "file1" || s.Size != 0 || s.IsDir {
		t.Error("Got Wrong Stats", s)
	}

	RemoveTempFile("file1")
}

func TestAttr_Failure(t *testing.T) {
	payload := &RemotePath{
		Hostname: "localhost",
		Port: 11211,
		Path: "/tmp/file1",
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

func TestReadDir_Success(t *testing.T) {

	CreateTempDir("dir1")
	CreateTempFile("dir1/file1")
	CreateTempFile("dir1/file2")

	payload := &RemotePath{
		Hostname: "localhost",
		Port: 11211,
		Path: "/tmp/dir1",
	}

	pkt := CreatePacket(ReadDirRequest, payload)

	stats, err := ReadDir(pkt)

	if err != nil {
		t.Error("Got Error in ReadDir",err)
	}

	arr := stats.Stats

	if len(arr) != 2 {
		t.Error("Unknown Files returned", arr)
	}

	RemoveTempFile("dir1/file1")
	RemoveTempFile("dir1/file2")
	RemoveTempFile("dir1")
}