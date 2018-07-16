package ifs

import (
	"os"
	"io/ioutil"

	"time"
	"sync"
	"path"
	"go.uber.org/zap"
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

func (fh *AgentFileHandler) convertReadDirOutput(files []os.FileInfo, err error) (*DirInfo, error) {
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

func (fh *AgentFileHandler) ReadDir(request *Packet) (*DirInfo, error) {

	readDirInfo := request.Data.(*ReadDirInfo)

	filePath := readDirInfo.Path

	zap.L().Debug("Processing ReadDir Request",
		zap.String("op", "readdir"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", filePath),
	)

	val, ok := fh.Opened.Load(readDirInfo.FileDescriptor)

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

func (fh *AgentFileHandler) ReadDirAll(request *Packet) (*DirInfo, error) {

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

func (fh *AgentFileHandler) FetchFile(request *Packet) (*FileChunk, error) {

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

		fileChunk.Compress()

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

func (fh *AgentFileHandler) ReadFile(request *Packet) (*FileChunk, error) {
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

func (fh *AgentFileHandler) WriteFile(request *Packet) (*WriteResult, error) {
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

	val, ok := fh.Opened.Load(writeInfo.FileDescriptor)

	if ok {

		f := val.(*os.File)

		n, err := f.WriteAt(writeInfo.Data, writeInfo.Offset)

		if err == nil {

			result := &WriteResult{
				Size: n,
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

func (fh *AgentFileHandler) SetAttr(request *Packet) error {
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

func (fh *AgentFileHandler) CreateFile(request *Packet) error {
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

		fh.Opened.Store(createInfo.FileDescriptor, f)

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

func (fh *AgentFileHandler) RemoveFile(request *Packet) error {
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

func (fh *AgentFileHandler) RenameFile(request *Packet) error {
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

func (fh *AgentFileHandler) OpenFile(request *Packet) error {
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

	fh.Opened.Store(openInfo.FileDescriptor, f)

	return nil
}

func (fh *AgentFileHandler) CloseFile(request *Packet) error {

	closeInfo := request.Data.(*CloseInfo)

	zap.L().Debug("Processing Close Request",
		zap.String("op", "close"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.String("path", closeInfo.Path),
		zap.Uint64("fd", closeInfo.FileDescriptor),
	)

	if val, ok := fh.Opened.Load(closeInfo.FileDescriptor); ok {
		f := val.(*os.File)
		f.Close()
		fh.Opened.Delete(closeInfo.FileDescriptor)
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
