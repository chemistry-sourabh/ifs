package arsyncfs

import (
	"os"
	"log"
	"io/ioutil"

	"io"
)

func Attr(request *Packet) *Stat {

	path := request.Data.(*RemotePath).Path

	log.Printf("Processing Attr Request %d for %s", request.Id, path)

	info, err := os.Lstat(path)

	if err != nil {
		log.Fatal(err.Error())
	}

	s := &Stat{}

	s.Name = info.Name()
	s.Size = info.Size()
	s.Mode = info.Mode()
	// Fix ModTime
	s.ModTime = info.ModTime().UnixNano()
	s.IsDir = info.IsDir()

	//sys := info.Sys().(syscall.Stat_t)

	return s

}

func ReadDir(request *Packet) *DirInfo {

	path := request.Data.(*RemotePath).Path

	dirInfo := &DirInfo{}

	var stats []*Stat

	log.Printf("Processing ReadDir Request %d for %s", request.Id, path)

	files, err := ioutil.ReadDir(path)

	if err != nil {
		log.Fatal(err)
	}


	for _, file := range files {

		s := &Stat{}

		s.Name = file.Name()
		s.Size = file.Size()
		s.Mode = file.Mode()
		s.ModTime = file.ModTime().UnixNano()
		s.IsDir = file.IsDir()

		stats = append(stats, s)

	}

	dirInfo.Stats = stats

	return dirInfo

}

func FetchFile(request *Packet) *FileChunk  {

	path := request.Data.(*RemotePath).Path
	data, err := ioutil.ReadFile(path)

	log.Printf("Processing FetchFile Request %d for %s", request.Id, path)

	if err != nil {
		log.Fatal(err)
	}

	fileChunk := &FileChunk{
		Chunk: data,
		Size: -1, // Invalid
		Err: nil,
	}

	return fileChunk
}

func ReadFile(request *Packet) *FileChunk {
	readInfo := request.Data.(*ReadInfo)

	path := readInfo.RemotePath.Path

	f, err := os.OpenFile(path, os.O_RDONLY, 0666)

	if err != nil {
		log.Fatal(err)
	}

	b := make([]byte, readInfo.Size)
	n, err := f.ReadAt(b, readInfo.Offset)

	fileChunk := &FileChunk{
		Chunk: b,
		Size: n,
	}

	if err != nil {

		if err != io.EOF {
			log.Fatal(err)
		} else {
			fileChunk.Err = err
		}
	}

	f.Close()

	return fileChunk
}

