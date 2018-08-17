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


package ifs

import (
	"os"
	"io/ioutil"

	"time"
	"sync"
	"path"
	"go.uber.org/zap"
	"github.com/orcaman/concurrent-map"
	"strconv"
	"io"
)

type agentFileHandler struct {
	Opened cmap.ConcurrentMap
}

var (
	agentFileHandlerInstance *agentFileHandler
	agentFileHandlerOnce sync.Once
)

func AgentFileHandler() *agentFileHandler {
	agentFileHandlerOnce.Do(func() {
		agentFileHandlerInstance = &agentFileHandler{
			Opened: cmap.New(),
		}
	})

	return agentFileHandlerInstance
}

func (fh *agentFileHandler) CloseAll() error {
	for t := range fh.Opened.IterBuffered() {
		f := t.Val.(*os.File)
		f.Close()
		fh.Opened.Remove(t.Key)
	}

	zap.L().Debug("Closed All Open Files")

	return nil
}

func (fh *agentFileHandler) Attr(request *Packet) (*Stat, error) {

	filePath := request.Data.(*RemotePath).Path

	zap.L().Debug("Processing Attr Request",
		zap.String("op", "attr"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
	)

	info, err := os.Lstat(filePath)

	if err == nil {
		s := &Stat{}
		s.Name = info.Name()
		s.Size = info.Size()
		s.Mode = info.Mode()
		s.ModTime = info.ModTime().UnixNano()
		s.IsDir = info.IsDir()

		zap.L().Debug("Attr Response",
			zap.String("op", "attr"),
			zap.Uint8("conn_id", request.ConnId),
			zap.Bool("request", request.IsRequest()),
			zap.Uint64("id", request.Id),
			zap.String("path", filePath),
			zap.String("mode", s.Mode.String()),
			zap.Int64("size", s.Size),
			zap.Time("mtime", time.Unix(0, s.ModTime)),
		)

		return s, nil
	} else {
		err = ConvertErr(err)

		zap.L().Warn("Attr Error Response",
			zap.String("op", "attr"),
			zap.Uint8("conn_id", request.ConnId),
			zap.Bool("request", request.IsRequest()),
			zap.Uint64("id", request.Id),
			zap.String("path", filePath),
			zap.Error(err),
		)
	}

	return nil, err

}

func (fh *agentFileHandler) convertReadDirOutput(files []os.FileInfo, err error) (*DirInfo, error) {
	if err == nil {

		var stats []*Stat
		dirInfo := &DirInfo{}

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

		return dirInfo, nil
	}

	err = ConvertErr(err)

	return nil, err
}

func (fh *agentFileHandler) ReadDir(request *Packet) (*DirInfo, error) {

	readDirInfo := request.Data.(*ReadDirInfo)

	filePath := readDirInfo.Path

	zap.L().Debug("Processing ReadDir Request",
		zap.String("op", "readdir"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
	)

	val, ok := fh.Opened.Get(strconv.FormatUint(readDirInfo.FileDescriptor, 10))

	if ok {

		f := val.(*os.File)

		files, err := f.Readdir(-1)

		dirInfo, err := fh.convertReadDirOutput(files, err)

		if err == nil {

			zap.L().Debug("ReadDir Response",
				zap.String("op", "attr"),
				zap.Uint8("conn_id", request.ConnId),
				zap.Bool("request", request.IsRequest()),
				zap.Uint64("id", request.Id),
				zap.String("path", filePath),
				zap.Int("size", len(dirInfo.Stats)),
			)

		} else {

			zap.L().Warn("ReadDir Error Response",
				zap.String("op", "attr"),
				zap.Uint8("conn_id", request.ConnId),
				zap.Bool("request", request.IsRequest()),
				zap.Uint64("id", request.Id),
				zap.String("path", filePath),
				zap.Error(err),
			)
		}

		return dirInfo, err
	}

	return nil, os.ErrInvalid
}

func (fh *agentFileHandler) ReadDirAll(request *Packet) (*DirInfo, error) {

	remotePath := request.Data.(*RemotePath)

	filePath := remotePath.Path

	zap.L().Debug("Processing ReadDirAll Request",
		zap.String("op", "readdirall"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
	)

	files, err := ioutil.ReadDir(filePath)

	dirInfo, err := fh.convertReadDirOutput(files, err)

	if err == nil {

		zap.L().Debug("ReadDirAll Response",
			zap.String("op", "readdirall"),
			zap.Uint8("conn_id", request.ConnId),
			zap.Bool("request", request.IsRequest()),
			zap.Uint64("id", request.Id),
			zap.String("path", filePath),
			zap.Int("size", len(dirInfo.Stats)),
		)

	} else {

		zap.L().Warn("ReadDirAll Error Response",
			zap.String("op", "readdirall"),
			zap.Uint8("conn_id", request.ConnId),
			zap.Bool("request", request.IsRequest()),
			zap.Uint64("id", request.Id),
			zap.String("path", filePath),
			zap.Error(err),
		)

	}

	return dirInfo, err
}

func (fh *agentFileHandler) FetchFile(request *Packet) (*FileChunk, error) {

	filePath := request.Data.(*RemotePath).Path

	zap.L().Debug("Processing Fetch Request",
		zap.String("op", "fetch"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
	)

	data, err := ioutil.ReadFile(filePath)

	if err == nil {

		fileChunk := &FileChunk{
			Chunk: data,
			Size:  len(data),
		}

		//fileChunk.Compress()

		zap.L().Debug("Fetch Response",
			zap.String("op", "fetch"),
			zap.Uint8("conn_id", request.ConnId),
			zap.Bool("request", request.IsRequest()),
			zap.Uint64("id", request.Id),
			zap.String("path", filePath),
			zap.Int("size", len(data)),
			zap.Int("compressed_size", len(fileChunk.Chunk)),
		)

		return fileChunk, err

	} else {
		err = ConvertErr(err)

		zap.L().Warn("Fetch Error Response",
			zap.String("op", "fetch"),
			zap.Uint8("conn_id", request.ConnId),
			zap.Bool("request", request.IsRequest()),
			zap.Uint64("id", request.Id),
			zap.String("path", filePath),
			zap.Error(err),
		)

	}

	return nil, err
}

func (fh *agentFileHandler) ReadFile(request *Packet) (*FileChunk, error) {
	readInfo := request.Data.(*ReadInfo)
	filePath := readInfo.Path

	zap.L().Debug("Processing Read Request",
		zap.String("op", "read"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
		zap.Uint64("fd", readInfo.FileDescriptor),
		zap.Int("size", readInfo.Size),
		zap.Int64("offset", readInfo.Offset),
	)

	val, ok := fh.Opened.Get(strconv.FormatUint(readInfo.FileDescriptor, 10))

	if ok {

		f := val.(*os.File)

		b := make([]byte, readInfo.Size)
		n, err := f.ReadAt(b, readInfo.Offset)

		if err == nil || ( err == io.EOF && n > 0 ) {
			fileChunk := &FileChunk{
				Chunk: b[:n],
				Size:  n,
			}

			//fileChunk.Compress()

			zap.L().Debug("Read Response",
				zap.String("op", "read"),
				zap.Uint8("conn_id", request.ConnId),
				zap.Bool("request", request.IsRequest()),
				zap.Uint64("id", request.Id),
				zap.String("path", filePath),
				zap.Uint64("fd", readInfo.FileDescriptor),
				zap.Int("size", readInfo.Size),
				zap.Int64("offset", readInfo.Offset),
				zap.Int("chunk_size", n),
				zap.Int("compressed_size", len(fileChunk.Chunk)),
			)

			return fileChunk, nil
		} else {
			err = ConvertErr(err)

			zap.L().Warn("Read Error Response",
				zap.String("op", "read"),
				zap.Uint8("conn_id", request.ConnId),
				zap.Bool("request", request.IsRequest()),
				zap.Uint64("id", request.Id),
				zap.String("path", filePath),
				zap.Uint64("fd", readInfo.FileDescriptor),
				zap.Int("size", readInfo.Size),
				zap.Int64("offset", readInfo.Offset),
				zap.Error(err),
			)

		}

		return nil, err
	}

	zap.L().Warn("Read Error Response",
		zap.String("op", "read"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
		zap.Uint64("fd", readInfo.FileDescriptor),
		zap.Int("size", readInfo.Size),
		zap.Int64("offset", readInfo.Offset),
		zap.Error(os.ErrInvalid),
	)

	return nil, os.ErrInvalid
}

func (fh *agentFileHandler) WriteFile(request *Packet) (*WriteResult, error) {
	writeInfo := request.Data.(*WriteInfo)
	filePath := writeInfo.Path

	zap.L().Debug("Processing Write Request",
		zap.String("op", "write"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
		zap.Uint64("fd", writeInfo.FileDescriptor),
		zap.Int64("offset", writeInfo.Offset),
		zap.Int("size", len(writeInfo.Data)),
	)

	val, ok := fh.Opened.Get(strconv.FormatUint(writeInfo.FileDescriptor, 10))

	if ok {

		f := val.(*os.File)

		n, err := f.WriteAt(writeInfo.Data, writeInfo.Offset)

		if err == nil {

			s, _ := os.Lstat(filePath)

			result := &WriteResult{
				Size: n,
				FileSize: s.Size(),
			}

			zap.L().Debug("Write Response",
				zap.String("op", "write"),
				zap.Uint8("conn_id", request.ConnId),
				zap.Bool("request", request.IsRequest()),
				zap.Uint64("id", request.Id),
				zap.String("path", filePath),
				zap.Uint64("fd", writeInfo.FileDescriptor),
				zap.Int64("offset", writeInfo.Offset),
				zap.Int("size", len(writeInfo.Data)),
				zap.Int("written", n),
				zap.Int64("file_size", s.Size()),
			)

			return result, nil
		} else {
			err = ConvertErr(err)

			zap.L().Warn("Write Error Response",
				zap.String("op", "write"),
				zap.Uint8("conn_id", request.ConnId),
				zap.Bool("request", request.IsRequest()),
				zap.Uint64("id", request.Id),
				zap.String("path", filePath),
				zap.Uint64("fd", writeInfo.FileDescriptor),
				zap.Int64("offset", writeInfo.Offset),
				zap.Int("size", len(writeInfo.Data)),
				zap.Error(err),
			)
		}

		return nil, err
	}

	zap.L().Warn("Write Error Response",
		zap.String("op", "write"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
		zap.Uint64("fd", writeInfo.FileDescriptor),
		zap.Int64("offset", writeInfo.Offset),
		zap.Int("size", len(writeInfo.Data)),
		zap.Error(os.ErrInvalid),
	)

	return nil, os.ErrInvalid
}

func (fh *agentFileHandler) SetAttr(request *Packet) error {
	attrInfo := request.Data.(*AttrInfo)
	filePath := attrInfo.Path

	zap.L().Debug("Processing SetAttr Request",
		zap.String("op", "setattr"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
		zap.String("valid", attrInfo.Valid.String()),
		zap.Uint64("size", attrInfo.Size),
		zap.Time("mtime", time.Unix(0, attrInfo.MTime)),
		zap.Time("atime", time.Unix(0, attrInfo.ATime)),
		zap.String("mode", attrInfo.Mode.String()),
	)

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

		zap.L().Warn("SetAttr Error Response",
			zap.String("op", "setattr"),
			zap.Uint8("conn_id", request.ConnId),
			zap.Bool("request", request.IsRequest()),
			zap.Uint64("id", request.Id),
			zap.String("path", filePath),
			zap.String("valid", attrInfo.Valid.String()),
			zap.Uint64("size", attrInfo.Size),
			zap.Time("mtime", time.Unix(0, attrInfo.MTime)),
			zap.Time("atime", time.Unix(0, attrInfo.ATime)),
			zap.String("mode", attrInfo.Mode.String()),
			zap.Error(err),
		)

	}

	return err
}

func (fh *agentFileHandler) CreateFile(request *Packet) error {
	createInfo := request.Data.(*CreateInfo)
	filePath := path.Join(createInfo.BaseDir, createInfo.Name)

	zap.L().Debug("Processing Create Request",
		zap.String("op", "create"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
		zap.String("name", createInfo.Name),
		zap.String("base_dir", createInfo.BaseDir),
		zap.Bool("is_dir", createInfo.IsDir),
	)

	if !createInfo.IsDir {
		f, err := os.Create(filePath)
		if err != nil {
			err = ConvertErr(err)

			zap.L().Warn("Create Error Response",
				zap.String("op", "create"),
				zap.Uint8("conn_id", request.ConnId),
				zap.Bool("request", request.IsRequest()),
				zap.Uint64("id", request.Id),
				zap.String("path", filePath),
				zap.String("name", createInfo.Name),
				zap.String("base_dir", createInfo.BaseDir),
				zap.Bool("is_dir", createInfo.IsDir),
				zap.Error(err),
			)

		}

		fh.Opened.Set(strconv.FormatUint(createInfo.FileDescriptor, 10), f)

		return err
	} else {
		err := os.Mkdir(filePath, 0755)

		if err != nil {
			err = ConvertErr(err)

			zap.L().Warn("Create Error Response",
				zap.String("op", "create"),
				zap.Uint8("conn_id", request.ConnId),
				zap.Bool("request", request.IsRequest()),
				zap.Uint64("id", request.Id),
				zap.String("path", filePath),
				zap.String("name", createInfo.Name),
				zap.String("base_dir", createInfo.BaseDir),
				zap.Bool("is_dir", createInfo.IsDir),
				zap.Error(err),
			)

		}

		return err
	}
}

func (fh *agentFileHandler) RemoveFile(request *Packet) error {
	remotePath := request.Data.(*RemotePath)

	zap.L().Debug("Processing Remove Request",
		zap.String("op", "remove"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", remotePath.Path),
	)

	err := os.Remove(remotePath.Path)

	if err != nil {
		err = ConvertErr(err)

		zap.L().Warn("Remove Error Response",
			zap.String("op", "remove"),
			zap.Uint8("conn_id", request.ConnId),
			zap.Bool("request", request.IsRequest()),
			zap.Uint64("id", request.Id),
			zap.String("path", remotePath.Path),
			zap.Error(err),
		)

	}

	return err
}

func (fh *agentFileHandler) RenameFile(request *Packet) error {
	renameInfo := request.Data.(*RenameInfo)

	zap.L().Debug("Processing Rename Request",
		zap.String("op", "rename"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", renameInfo.Path),
		zap.String("dest_path", renameInfo.DestPath),
	)

	err := os.Rename(renameInfo.Path, renameInfo.DestPath)

	if err != nil {
		err = ConvertErr(err)

		zap.L().Warn("Rename Error Response",
			zap.String("op", "rename"),
			zap.Uint8("conn_id", request.ConnId),
			zap.Bool("request", request.IsRequest()),
			zap.Uint64("id", request.Id),
			zap.String("path", renameInfo.Path),
			zap.String("dest_path", renameInfo.DestPath),
			zap.Error(err),
		)

	}

	return err
}

func (fh *agentFileHandler) OpenFile(request *Packet) error {
	openInfo := request.Data.(*OpenInfo)

	zap.L().Debug("Processing Open Request",
		zap.String("op", "open"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", openInfo.Path),
		zap.Uint64("fd", openInfo.FileDescriptor),
		zap.String("flags", openInfo.Flags.String()),
	)

	f, err := os.OpenFile(openInfo.Path, int(openInfo.Flags), 0666)

	if err != nil {

		zap.L().Warn("Open Error Response",
			zap.String("op", "open"),
			zap.Uint8("conn_id", request.ConnId),
			zap.Bool("request", request.IsRequest()),
			zap.Uint64("id", request.Id),
			zap.String("path", openInfo.Path),
			zap.Uint64("fd", openInfo.FileDescriptor),
			zap.String("flags", openInfo.Flags.String()),
			zap.Error(err),
		)

		return err
	}

	fh.Opened.Set(strconv.FormatUint(openInfo.FileDescriptor, 10), f)

	return nil
}

func (fh *agentFileHandler) CloseFile(request *Packet) error {

	closeInfo := request.Data.(*CloseInfo)

	zap.L().Debug("Processing Close Request",
		zap.String("op", "close"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", closeInfo.Path),
		zap.Uint64("fd", closeInfo.FileDescriptor),
	)

	if val, ok := fh.Opened.Get(strconv.FormatUint(closeInfo.FileDescriptor, 10)); ok {
		f := val.(*os.File)
		f.Close()
		fh.Opened.Remove(strconv.FormatUint(closeInfo.FileDescriptor, 10))
		return nil
	}

	zap.L().Debug("Close Error Response",
		zap.String("op", "close"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", closeInfo.Path),
		zap.Uint64("fd", closeInfo.FileDescriptor),
		zap.Error(os.ErrInvalid),
	)

	return os.ErrInvalid
}
