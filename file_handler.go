package ifs

import (
	"path"
	log "github.com/sirupsen/logrus"
	"os"
	"sync/atomic"
	"sync"
)

type FileHandler struct {
	Ifs            *Ifs
	FileDescriptor uint64
	Opened         *sync.Map
}

func (fh *FileHandler) StartUp() {
	log.Info("Starting File Handler")
	fh.Opened = &sync.Map{}
}

func (fh *FileHandler) OpenFile(remotePath *RemotePath, flags int, isDir bool) (uint64, error) {

	fd := atomic.AddUint64(&fh.FileDescriptor, 1)

	openInfo := &OpenInfo{
		FileDescriptor: fd,
		Path:     remotePath.Path,
		Flags:          flags,
	}

	if !isDir {
		go fh.Ifs.Hoarder.CacheOpen(remotePath, fd, flags)
	}

	resp := fh.Ifs.Talker.sendRequest(OpenRequest, remotePath.Hostname, openInfo)

	if err, ok := resp.Data.(Error); ok {
		return 0, err.Err
	}

	fh.Opened.Store(fd, openInfo)

	return fd, nil
}

// TODO Skip Cache if io op fails
func (fh *FileHandler) ReadData(handle *FileHandle, offset int64, size int) ([]byte, error) {

	if _, ok := fh.Opened.Load(handle.FileDescriptor); ok {

		data, err := fh.Ifs.Hoarder.ReadCache(handle.FileDescriptor, offset, size)

		// If Read from Cache Failed then get from remote
		if err != nil {
			// Should Ask Agent for bytes
			fileReadInfo := &ReadInfo{
				Path:           handle.RemoteNode.RemotePath.Path,
				FileDescriptor: handle.FileDescriptor,
				Offset:         offset,
				Size:           size,
			}

			resp := fh.Ifs.Talker.sendRequest(ReadFileRequest, handle.RemoteNode.RemotePath.Hostname, fileReadInfo)

			if err, ok := resp.Data.(Error); ok {
				return nil, err.Err
			} else {
				fileChunk := resp.Data.(*FileChunk)
				fileChunk.Decompress()
				return fileChunk.Chunk, nil
			}

		} else {
			return data, err
		}
	}

	return nil, os.ErrInvalid
}

func (fh *FileHandler) WriteData(handle *FileHandle, data []byte, offset int64) (int, error) {

	if _, ok := fh.Opened.Load(handle.FileDescriptor); ok {

		// Send Bytes to Agent
		writeInfo := &WriteInfo{
			Path:           handle.RemoteNode.RemotePath.Path,
			FileDescriptor: handle.FileDescriptor,
			Offset:         offset,
			Data:           data,
		}
		resp := fh.Ifs.Talker.sendRequest(WriteFileRequest, handle.RemoteNode.RemotePath.Hostname, writeInfo)
		if err, ok := resp.Data.(Error); ok {
			return 0, err.Err
		}

		writeResult := resp.Data.(*WriteResult)

		// TODO Log Error
		fh.Ifs.Hoarder.WriteCache(handle.FileDescriptor, offset, data)

		return writeResult.Size, nil
	}

	return 0, os.ErrNotExist
}

func (fh *FileHandler) Truncate(remotePath *RemotePath, attrInfo *AttrInfo) error {

	resp := fh.Ifs.Talker.sendRequest(SetAttrRequest, remotePath.Hostname, attrInfo)

	if err, ok := resp.Data.(Error); ok {
		return err.Err
	}

	fh.Ifs.Hoarder.CacheTrunc(remotePath, attrInfo)

	return nil
}

func (fh *FileHandler) Release(handle *FileHandle) error {
	if _, ok := fh.Opened.Load(handle.FileDescriptor); ok {

		closeInfo := &CloseInfo{
			FileDescriptor: handle.FileDescriptor,
			Path:           handle.RemoteNode.RemotePath.Path,
		}

		resp := fh.Ifs.Talker.sendRequest(CloseRequest, handle.RemoteNode.RemotePath.Hostname, closeInfo)

		if err, ok := resp.Data.(Error); ok {
			return err.Err
		}

		fh.Ifs.Hoarder.CacheClose(handle.FileDescriptor)

		fh.Opened.Delete(handle.FileDescriptor)

		return nil
	}

	return os.ErrNotExist
}

func (fh *FileHandler) Create(remotePath *RemotePath, name string) (uint64, error) {

	fd := atomic.AddUint64(&fh.FileDescriptor, 1)

	req := &CreateInfo{
		BaseDir:        remotePath.Path,
		Name:           name,
		IsDir:          false,
		FileDescriptor: fd,
	}

	resp := fh.Ifs.Talker.sendRequest(CreateRequest, remotePath.Hostname, req)

	if err, ok := resp.Data.(Error); ok {
		return 0, err.Err
	}

	newRemotePath := &RemotePath{
		Hostname: remotePath.Hostname,
		Port:     remotePath.Port,
		Path:     path.Join(remotePath.Path, name),
	}

	fh.Ifs.Hoarder.CacheCreate(newRemotePath, fd)

	fh.Opened.Store(fd, req)

	return fd, nil
}

func (fh *FileHandler) Mkdir(remotePath *RemotePath, name string) error {
	req := &CreateInfo{
		BaseDir: remotePath.Path,
		Name:    name,
		IsDir:   true,
	}

	resp := fh.Ifs.Talker.sendRequest(CreateRequest, remotePath.Hostname, req)

	if err, ok := resp.Data.(Error); ok {
		return err.Err
	}

	return nil
}

func (fh *FileHandler) Remove(remotePath *RemotePath, name string) error {

	newRemotePath := &RemotePath{
		Hostname: remotePath.Hostname,
		Port:     remotePath.Port,
		Path:     path.Join(remotePath.Path, name),
	}

	resp := fh.Ifs.Talker.sendRequest(RemoveRequest, remotePath.Hostname, newRemotePath)

	if err, ok := resp.Data.(Error); ok {
		return err.Err
	}

	fh.Ifs.Hoarder.CacheDelete(remotePath)

	return nil
}
func (fh *FileHandler) Rename(remotePath *RemotePath, destPath string) error {

	req := &RenameInfo{
		Path:     remotePath.Path,
		DestPath: destPath,
	}

	resp := fh.Ifs.Talker.sendRequest(RenameRequest, remotePath.Hostname, req)

	if err, ok := resp.Data.(Error); ok {
		return err.Err
	}

	fh.Ifs.Hoarder.CacheRename(remotePath, destPath)

	return nil
}

//func (fh *FileHandler) Flush(handle *FileHandle) error {
//	req := &FlushInfo{
//		RemotePath: handle.RemoteNode.RemotePath,
//		FileDescriptor: handle.FileDescriptor,
//	}
//
//	resp := fh.Ifs.Talker.sendRequest(FlushRequest, req)
//
//	if err, ok := resp.Data.(Error); ok {
//		return err.Err
//	}
//
//	fh.Ifs.Hoarder.CacheFlush(handle.FileDescriptor)
//
//	return nil
//}
