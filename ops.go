package arsyncfs

import (
	"os"
	"log"
	"io/ioutil"
	//"encoding/base64"
	"encoding/base64"
)

func Attr(request *Request) *StatResponse {

	resp := &StatResponse{
		RequestId: request.Id,
		RNode:     request.RemoteNode,
	}

	rn := request.RemoteNode
	path := rn.RemotePath.Path

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

	resp.Stat = s

	return resp

}

func ReadDir(request *Request) *ReadDirResponse {

	path := request.RemoteNode.RemotePath.Path

	resp := &ReadDirResponse{
		RequestId: request.Id,
		RNode:     request.RemoteNode,
	}

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

	resp.Stats = stats

	return resp

}

func FetchFile(request *Request) BaseResponse {
	data, err := ioutil.ReadFile(request.RemoteNode.RemotePath.Path)

	if err != nil {
		log.Fatal(err)
	}

	resp := &FileDataResponse{
		RequestId: request.Id,
		RNode:     request.RemoteNode,
	}

	if len(data) > 0 {
		resp.Data = base64.StdEncoding.EncodeToString(data)
	} else {
		resp.Data = ""
	}

	log.Println(resp.Data)

	return resp
}

