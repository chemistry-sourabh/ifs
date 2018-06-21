package ifs

import (
	"os"
	log "github.com/sirupsen/logrus"
	"io/ioutil"

	"path"
	"time"
	"sync"
)

type AgentFileHandler struct {
	Opened sync.Map
	//Opened map[uint64]*os.File
}

func NewAgentFileHandler() *AgentFileHandler {
	return &AgentFileHandler{
		Opened: sync.Map{},
	}
}

func (fh *AgentFileHandler) Attr(request *Packet) (*Stat, error) {

	filePath := request.Data.(*RemotePath).Path

	fields := log.Fields{
		"op":   "attr",
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
			"op":       "attr",
			"id":       request.Id,
			"path":     filePath,
			"mode":     s.Mode,
			"size":     s.Size,
			"mod_time": time.Unix(0, s.ModTime),
		}).Debug("Attr Response")

		return s, nil
	} else {
		err = ConvertErr(err)
		log.WithFields(fields).Error("Attr Error Response:", err)
	}

	return nil, err

}

func (fh *AgentFileHandler) ReadDir(request *Packet) (*DirInfo, error) {

	readDirInfo := request.Data.(*ReadDirInfo)

	filePath := readDirInfo.RemotePath.Path

	dirInfo := &DirInfo{}

	var stats []*Stat

	fields := log.Fields{
		"op":   "readdir",
		"id":   request.Id,
		"path": filePath,
	}

	log.WithFields(fields).Debug("Processing Readdir Request")

	val, ok := fh.Opened.Load(readDirInfo.FileDescriptor)

	if ok {

		f := val.(*os.File)

		files, err := f.Readdir(-1)

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

			err = ConvertErr(err)
			log.WithFields(fields).Error("Readdir Error Response:", err)
		}

		return nil, err
	}

	return nil, os.ErrInvalid
}

func (fh *AgentFileHandler) FetchFile(request *Packet) (*FileChunk, error) {

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
			"id":              request.Id,
			"path":            filePath,
			"size":            len(data),
			"compressed_size": len(fileChunk.Chunk),
		}).Debug(" FetchFile Response")

		return fileChunk, err

	} else {
		err = ConvertErr(err)
		log.WithFields(fields).Warnf("FetchFile Error Response:", err)
	}

	return nil, err
}

func (fh *AgentFileHandler) ReadFile(request *Packet) (*FileChunk, error) {
	readInfo := request.Data.(*ReadInfo)
	filePath := readInfo.RemotePath.Path

	fields := log.Fields{
		"op":     "read",
		"id":     request.Id,
		"path":   filePath,
		"fd":     readInfo.FileDescriptor,
		"size":   readInfo.Size,
		"offset": readInfo.Offset,
	}

	log.WithFields(fields).Debug("Processing Read Request")

	val, ok := fh.Opened.Load(readInfo.FileDescriptor)

	if ok {

		f := val.(*os.File)

		b := make([]byte, readInfo.Size)
		n, err := f.ReadAt(b, readInfo.Offset)

		if err == nil {
			fileChunk := &FileChunk{
				Chunk: b,
				Size:  n,
			}

			fileChunk.Compress()

			log.WithFields(log.Fields{
				"op":              "read",
				"id":              request.Id,
				"path":            filePath,
				"size":            readInfo.Size,
				"offset":          readInfo.Offset,
				"chunk_size":      n,
				"compressed_size": len(fileChunk.Chunk),
			}).Debug("Read Response")

			return fileChunk, nil
		} else {
			err = ConvertErr(err)
			log.WithFields(fields).Warnf("Read Error Response:", err)
		}

		return nil, err
	}

	return nil, os.ErrInvalid
}

func (fh *AgentFileHandler) WriteFile(request *Packet) (*WriteResult, error) {
	writeInfo := request.Data.(*WriteInfo)
	filePath := writeInfo.RemotePath.Path

	fields := log.Fields{
		"op":     "write",
		"id":     request.Id,
		"path":   filePath,
		"fd":     writeInfo.FileDescriptor,
		"offset": writeInfo.Offset,
		"size":   len(writeInfo.Data),
	}

	log.WithFields(fields).Debug("Processing Write Request")

	val, ok := fh.Opened.Load(writeInfo.FileDescriptor)

	if ok {

		f := val.(*os.File)

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
			err = ConvertErr(err)
			log.WithFields(fields).Warnf("Write Error Response")
		}

		return nil, err
	}

	return nil, os.ErrInvalid
}

func (fh *AgentFileHandler) SetAttr(request *Packet) error {
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
		err = ConvertErr(err)
		log.WithFields(fields).Warnf("SetAttr Error Response:", err)
	}

	return err
}

func (fh *AgentFileHandler) CreateFile(request *Packet) error {
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
		f, err := os.Create(filePath)
		if err != nil {
			err = ConvertErr(err)
			log.WithFields(fields).Warnf("Create Error Response:", err)
		}

		fh.Opened.Store(createInfo.FileDescriptor, f)

		return err
	} else {
		err := os.Mkdir(filePath, 0755)

		if err != nil {
			err = ConvertErr(err)
			log.WithFields(fields).Warnf("Create Error Response:", err)
		}

		return err
	}
}

func (fh *AgentFileHandler) RemoveFile(request *Packet) error {
	remotePath := request.Data.(*RemotePath)

	fields := log.Fields{
		"op":   "remove",
		"id":   request.Id,
		"path": remotePath.Path,
	}

	log.WithFields(fields).Debug("Processing Remove Request")

	err := os.Remove(remotePath.Path)

	if err != nil {
		err = ConvertErr(err)
		log.WithFields(fields).Warnf("Remove Error Response:", err)
	}

	return err
}

func (fh *AgentFileHandler) RenameFile(request *Packet) error {
	renameInfo := request.Data.(*RenameInfo)

	fields := log.Fields{
		"op":       "rename",
		"id":       request.Id,
		"path":     renameInfo.RemotePath.Path,
		"new_path": renameInfo.DestPath,
	}

	log.WithFields(fields).Debug("Processing Rename Request")

	err := os.Rename(renameInfo.RemotePath.Path, renameInfo.DestPath)

	if err != nil {
		err = ConvertErr(err)
		log.WithFields(fields).Warnf("Rename Error Response:", err)
	}

	return err
}

func (fh *AgentFileHandler) OpenFile(request *Packet) error {
	openInfo := request.Data.(*OpenInfo)

	fields := log.Fields{
		"op":              "open",
		"id":              request.Id,
		"path":            openInfo.RemotePath.Path,
		"file_descriptor": openInfo.FileDescriptor,
		"flags":           openInfo.Flags,
	}

	log.WithFields(fields).Debug("Processing Open Request")

	f, err := os.OpenFile(openInfo.RemotePath.Path, openInfo.Flags, 0666)

	if err != nil {
		return err
	}

	fh.Opened.Store(openInfo.FileDescriptor, f)

	return nil
}

func (fh *AgentFileHandler) CloseFile(request *Packet) error {

	closeInfo := request.Data.(*CloseInfo)

	fields := log.Fields{
		"op":              "close",
		"id":              request.Id,
		"path":            closeInfo.RemotePath.Path,
		"file_descriptor": closeInfo.FileDescriptor,
	}

	log.WithFields(fields).Debug("Processing Close Request")

	if val, ok := fh.Opened.Load(closeInfo.FileDescriptor); ok {
		f := val.(*os.File)
		f.Close()
		fh.Opened.Delete(closeInfo.FileDescriptor)
	}

	return nil
}
