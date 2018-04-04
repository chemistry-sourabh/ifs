package arsyncfs

import (
	"os"
	"log"
	"io/ioutil"
	"encoding/base64"
)

func Attr(request *Request) *Stat {

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
	s.ModTime = info.ModTime()
	s.IsDir = info.IsDir()

	//sys := info.Sys().(syscall.Stat_t)

	return s

}

func ReadDir(request *Request) *[]Stat {

	path := request.RemoteNode.RemotePath.Path

	var stats []Stat

	log.Printf("Processing ReadDir Request %d for %s", request.Id, path)

	files, err := ioutil.ReadDir(path)

	if err != nil {
		log.Fatal(err)
	}


	for _, file := range files {

		s := Stat{}

		//fmt.Println(reflect.TypeOf(file))

		s.Name = file.Name()
		s.Size = file.Size()
		s.Mode = file.Mode()
		s.ModTime = file.ModTime()
		s.IsDir = file.IsDir()

		stats = append(stats, s)

	}


	return &stats

}

func FetchFile(request *Request) string {
	data, err := ioutil.ReadFile(request.RemoteNode.RemotePath.Path)

	if err != nil {
		log.Fatal(err)
	}

	if len(data) > 0 {
		return base64.StdEncoding.EncodeToString(data)
	} else {
		return ""
	}

}

