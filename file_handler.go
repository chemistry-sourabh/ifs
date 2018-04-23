package ifs

import (
	"path"
	log "github.com/sirupsen/logrus"
	"os"
	"unsafe"
)

type FileHandler struct {
	Ifs    *Ifs
	Opened *FastMap
}

func (fh *FileHandler) StartUp() {
	log.Info("Starting File Handler")
	fh.Opened = NewFastMap()
}

func (fh *FileHandler) OpenFile(remotePath *RemotePath) error {

	var err error

	fh.Ifs.Hoarder.SubmitRequest(CacheFileRequest, remotePath)
	fh.Opened.Set(remotePath.String(), true)

	return err
}

//func (fh *FileHandler) checkCacheSpace() bool {
//	// TODO Implement properly
//	return fh.Size > 0
//}

//func (fh *FileHandler) convertToCacheName(path *RemotePath) string {
//	s := strings.Replace(path.String(), "/", "_", -1)
//	s = strings.Replace(s, ":", "_", 1)
//	s = strings.Replace(s, "@", "_", 1)
//	return s
//}

// TODO Skip Cache if io op fails
func (fh *FileHandler) ReadData(remotePath *RemotePath, offset int64, size int) ([]byte, error) {

	// TODO  Check if File is Open
	data, err := fh.Ifs.Hoarder.ReadCache(remotePath, offset, size)

	// If Read from Cache Failed then get from remote
	if err != nil {
		// Should Ask Agent for bytes
		fileReadInfo := &ReadInfo{
			RemotePath: remotePath,
			Offset:     offset,
			Size:       size,
		}

		resp := fh.Ifs.Talker.sendRequest(ReadFileRequest, fileReadInfo)

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

func (fh *FileHandler) ReadAllData(remotePath *RemotePath) ([]byte, error) {

	data, err := fh.Ifs.Hoarder.ReadAllCache(remotePath)

	if err != nil {

		resp := fh.Ifs.Talker.sendRequest(FetchFileRequest, remotePath)

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

// TODO Parallize Cache and Remote Writes
func (fh *FileHandler) WriteData(remotePath *RemotePath, data []byte, offset int64) (int, error) {

	n, err := fh.Ifs.Hoarder.WriteCache(remotePath, offset, data)

	if err != nil {

		// Send Bytes to Agent
		writeInfo := &WriteInfo{
			RemotePath: remotePath,
			Offset:     offset,
			Data:       data,
		}
		resp := fh.Ifs.Talker.sendRequest(WriteFileRequest, writeInfo)
		if err, ok := resp.Data.(Error); ok {
			return 0, err.Err
		} else {
			writeResult := resp.Data.(*WriteResult)
			return writeResult.Size, nil
		}

	}

	return n, err

}

func (fh *FileHandler) Truncate(attrInfo *AttrInfo) error {

	fh.Ifs.Hoarder.SubmitRequest(CacheTruncRequest, attrInfo)

	resp := fh.Ifs.Talker.sendRequest(SetAttrRequest, attrInfo)

	if err, ok := resp.Data.(Error); ok {
		return err.Err
	}

	return nil
}

func (fh *FileHandler) Release(remotePath *RemotePath) error {
	if _, ok := fh.Opened.Load(remotePath.String()); ok {
		fh.Opened.Delete(remotePath.String())
		return nil
	}

	return os.ErrNotExist
}

func (fh *FileHandler) Create(remotePath *RemotePath, name string) error {

	req := &CreateInfo{
		BaseDir: remotePath,
		Name:    name,
		IsDir:   false,
	}

	resp := fh.Ifs.Talker.sendRequest(CreateRequest, req)

	newRemotePath := &RemotePath{
		Hostname: remotePath.Hostname,
		Port:     remotePath.Port,
		Path:     path.Join(remotePath.Path, name),
	}

	fh.Ifs.Hoarder.SubmitRequest(CacheCreateRequest, newRemotePath)

	val := true
	fh.Opened.Set(newRemotePath.String(), unsafe.Pointer(&val))

	if err, ok := resp.Data.(Error); ok {
		return err.Err
	}

	return nil
}

func (fh *FileHandler) Mkdir(remotePath *RemotePath, name string) error {
	req := &CreateInfo{
		BaseDir: remotePath,
		Name:    name,
		IsDir:   true,
	}

	resp := fh.Ifs.Talker.sendRequest(CreateRequest, req)

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

	resp := fh.Ifs.Talker.sendRequest(RemoveRequest, newRemotePath)

	fh.Ifs.Hoarder.SubmitRequest(CacheDeleteRequest, newRemotePath)

	if err, ok := resp.Data.(Error); ok {
		return err.Err
	}

	return nil
}
