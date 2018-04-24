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
		"op":   "attr",
		"id":   request.Id,
		"path": filePath,
	}
	log.WithFields(fields).Debug("Processing Attr Request")

	info, pathError := os.Lstat(filePath)

	err := pathError.(*os.PathError).Err

	if err == nil {
		s := &Stat{}
		s.Name = info.Name()
		s.Size = info.Size()
		s.Mode = info.Mode()
		s.ModTime = info.ModTime().UnixNano()
		s.IsDir = info.IsDir()

		log.WithFields(log.Fields{
			"op":       "attr",
			"id":       request.Id,
			"path":     filePath,
			"mode":     s.Mode,
			"size":     s.Size,
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
		"op":   "readdir",
		"id":   request.Id,
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
			"op":   "readdir",
			"id":   request.Id,
			"path": filePath,
			"size": len(stats),
		}).Debug("Readdir Response")

		return dirInfo, nil
	} else {
		log.WithFields(fields).Error("Readdir Error Response:", err)
	}

	return nil, err
}

func FetchFile(request *Packet) (*FileChunk, error) {

	filePath := request.Data.(*RemotePath).Path

	fields := log.Fields{
		"op":   "fetch",
		"id":   request.Id,
		"path": filePath,
	}

	log.WithFields(fields).Debug("Processing FetchFile Request")

	data, err := ioutil.ReadFile(filePath)

	if err == nil {

		fileChunk := &FileChunk{
			Chunk: data,
			Size:  len(data),
		}

		fileChunk.Compress()

		log.WithFields(log.Fields{
			"id":   request.Id,
			"path": filePath,
			"size": len(data),
			"compressed_size": len(fileChunk.Chunk),
		}).Debug(" FetchFile Response")

		return fileChunk, err

	} else {
		log.WithFields(fields).Errorf("FetchFile Error Response:", err)
	}

	return nil, err
}

func ReadFile(request *Packet) (*FileChunk, error) {
	readInfo := request.Data.(*ReadInfo)
	filePath := readInfo.RemotePath.Path

	fields := log.Fields{
		"op":     "read",
		"id":     request.Id,
		"path":   filePath,
		"size":   readInfo.Size,
		"offset": readInfo.Offset,
	}

	log.WithFields(fields).Debug("Processing Read Request")

	f, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
	defer f.Close()

	// Should be there because the file is already opened
	if err != nil {
		log.WithFields(fields).Fatal("Fatal Error:", err)
	}

	b := make([]byte, readInfo.Size)
	n, err := f.ReadAt(b, readInfo.Offset)

	if err == nil {
		fileChunk := &FileChunk{
			Chunk: b,
			Size:  n,
		}

		fileChunk.Compress()

		log.WithFields(log.Fields{
			"op":         "read",
			"id":         request.Id,
			"path":       filePath,
			"size":       readInfo.Size,
			"offset":     readInfo.Offset,
			"chunk_size": n,
			"compressed_size": len(fileChunk.Chunk),
		}).Debug("Read Response")

		return fileChunk, nil
	} else {
		log.WithFields(fields).Errorf("Read Error Response:", err)
	}

	return nil, err
}

func WriteFile(request *Packet) (*WriteResult, error) {
	writeInfo := request.Data.(*WriteInfo)
	filePath := writeInfo.RemotePath.Path

	fields := log.Fields{
		"op":     "write",
		"id":     request.Id,
		"path":   filePath,
		"offset": writeInfo.Offset,
		"size":   len(writeInfo.Data),
	}

	log.WithFields(fields).Debug("Processing Write Request")

	f, err := os.OpenFile(filePath, os.O_WRONLY, 0666)
	defer f.Close()

	// Should be there because the file is already opened
	if err != nil {
		log.WithFields(fields).Fatal("Fatal Error:", err)
	}

	n, err := f.WriteAt(writeInfo.Data, writeInfo.Offset)

	if err == nil {

		result := &WriteResult{
			Size: n,
		}

		log.WithFields(log.Fields{
			"op":         "write",
			"id":         request.Id,
			"path":       filePath,
			"offset":     writeInfo.Offset,
			"chunk_size": len(writeInfo.Data),
			"size":       n,
		}).Debug("Write Response")

		return result, nil
	} else {
		log.WithFields(fields).Errorf("Write Error Response")
	}

	return nil, err
}

func SetAttr(request *Packet) error {
	attrInfo := request.Data.(*AttrInfo)
	filePath := attrInfo.RemotePath.Path

	fields := log.Fields{
		"op":    "setattr",
		"id":    request.Id,
		"path":  filePath,
		"valid": attrInfo.Valid.String(),
		"size":  attrInfo.Size,
		"mtime": attrInfo.MTime,
		"atime": attrInfo.ATime,
		"mode":  attrInfo.Mode,
	}

	log.WithFields(fields).Debug("Processing SetAttr Request")

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

	if err != nil {
		log.WithFields(fields).Errorf("SetAttr Error Response:", err)
	}

	return err
}

func CreateFile(request *Packet) error {
	createInfo := request.Data.(*CreateInfo)
	filePath := path.Join(createInfo.BaseDir.Path, createInfo.Name)

	fields := log.Fields{
		"op":       "create",
		"id":       request.Id,
		"path":     filePath,
		"name":     createInfo.Name,
		"base_dir": createInfo.BaseDir,
		"is_dir":   createInfo.IsDir,
	}

	log.WithFields(fields).Debug("Processing Create Request")

	if !createInfo.IsDir {
		f, pathError := os.Create(filePath)
		err := pathError.(*os.PathError).Err
		if err == nil {
			defer f.Close()
		} else {
			log.WithFields(fields).Errorf("Create Error Response:", err)
		}
		return err
	} else {
		err := os.Mkdir(filePath, 0755)

		if err != nil {
			log.WithFields(fields).Errorf("Create Error Response:", err)
		}

		return err
	}
}

func RemoveFile(request *Packet) error {
	remotePath := request.Data.(*RemotePath)

	fields := log.Fields{
		"op":   "remove",
		"id":   request.Id,
		"path": remotePath.Path,
	}

	log.WithFields(fields).Debug("Processing Remove Request")

	err := os.Remove(remotePath.Path)

	if err != nil {
		log.WithFields(fields).Debug("Remove Error Response:", err)
	}

	return err
}
