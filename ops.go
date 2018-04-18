package ifs

import (
	"os"
	log "github.com/sirupsen/logrus"
	"io/ioutil"

	"path"
	"time"
)

func Attr(request *Packet) (*Stat, error) {

	filePath := request.Data.(*RemotePath).Path

	fields := log.Fields{
		"id":   request.Id,
		"path": filePath,
	}
	log.WithFields(fields).Debug("Processing Attr Request")

	info, err := os.Lstat(filePath)

	if err == nil {
		s := &Stat{}
		s.Name = info.Name()
		s.Size = info.Size()
		s.Mode = info.Mode()
		s.ModTime = info.ModTime().UnixNano()
		s.IsDir = info.IsDir()

		log.WithFields(log.Fields{
			"id": request.Id,
			"path": filePath,
			"mode": s.Mode,
			"size": s.Size,
			"mod_time": time.Unix(0, s.ModTime),
		}).Debug("Attr Response")

		return s, nil
	} else {
		log.WithFields(fields).Error("Attr Error Response:", err)
	}

	return nil, err

}

func ReadDir(request *Packet) (*DirInfo, error) {

	filePath := request.Data.(*RemotePath).Path

	dirInfo := &DirInfo{}

	var stats []*Stat

	fields := log.Fields{
		"id": request.Id,
		"path": filePath,
	}

	log.WithFields(fields).Debug("Processing Readdir Request")

	files, err := ioutil.ReadDir(filePath)

	if err == nil {

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

		log.WithFields(log.Fields{
			"id":request.Id,
			"path": filePath,
			"size": len(stats),
		}).Debug("Readdir Error Response")

		return dirInfo, nil
	} else {
		log.WithFields(fields).Error("Readdir Error Response:",err)
	}

	return nil, err
}

func FetchFile(request *Packet) (*FileChunk, error) {

	filePath := request.Data.(*RemotePath).Path
	data, err := ioutil.ReadFile(filePath)

	log.Printf("Processing FetchFile Request %d for %s", request.Id, filePath)

	if err == nil {

		fileChunk := &FileChunk{
			Chunk: data,
			Size:  -1, // Invalid
		}

		return fileChunk, err

	}

	return nil, err
}

func ReadFile(request *Packet) (*FileChunk, error) {
	readInfo := request.Data.(*ReadInfo)

	filePath := readInfo.RemotePath.Path

	f, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	defer f.Close()

	// Should be there because the file is already opened
	if err != nil {
		log.Fatal(err)
	}

	b := make([]byte, readInfo.Size)
	n, err := f.ReadAt(b, readInfo.Offset)

	if err == nil {
		fileChunk := &FileChunk{
			Chunk: b,
			Size:  n,
		}

		return fileChunk, nil
	}

	return nil, err
}

func WriteFile(request *Packet) (*WriteResult, error) {
	writeInfo := request.Data.(*WriteInfo)

	filePath := writeInfo.RemotePath.Path

	f, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	defer f.Close()

	// Should be there because the file is already opened
	if err != nil {
		log.Fatal(err)
	}

	n, err := f.WriteAt(writeInfo.Data, writeInfo.Offset)

	if err == nil {

		result := &WriteResult{
			Size: n,
		}

		return result, nil
	}

	return nil, err
}

func SetAttr(request *Packet) error {
	attrInfo := request.Data.(*AttrInfo)

	filePath := attrInfo.RemotePath.Path

	var err error
	if attrInfo.Valid.Size() {
		err = os.Truncate(filePath, int64(attrInfo.Size))
	}

	if attrInfo.Valid.Mode() {
		err = os.Chmod(filePath, attrInfo.Mode)
	}

	// Assuming both are set at same time
	if attrInfo.Valid.Atime() || attrInfo.Valid.Mtime() {
		err = os.Chtimes(filePath, time.Unix(0, attrInfo.ATime), time.Unix(0, attrInfo.MTime))
	}

	return err
}

func CreateFile(request *Packet) error {
	createInfo := request.Data.(*CreateInfo)
	filePath := path.Join(createInfo.BaseDir.Path, createInfo.Name)

	if !createInfo.IsDir {
		f, err := os.Create(filePath)
		if err == nil {
			defer f.Close()
		}
		return err
	} else {
		return os.Mkdir(filePath, 0755)
	}
}

func RemoveFile(request *Packet) error {
	remotePath := request.Data.(*RemotePath)
	return os.Remove(remotePath.Path)
}
