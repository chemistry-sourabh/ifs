package arsyncfs

import (
	"io/ioutil"
	"path"
	"log"
	"os"
	"strings"
)

type FileHandler struct {
	Ifs    *Ifs
	Path   string
	Size   uint64
	Cached map[string]bool // TODO Come up with a hash scheme that will replace filenames along with collisions
	Opened map[string]bool
	//RequestChannel       chan *CacheRequest
	//EgressRequestChannel chan *Request
}

func (fh *FileHandler) StartUp() {
	log.Println("Starting File Handler")
	fh.DeleteCache()

	fh.Cached = make(map[string]bool)
	fh.Opened = make(map[string]bool)

}

func (fh *FileHandler) DeleteCache() {
	log.Println("Deleting Cache")
	os.RemoveAll(fh.Path)
	os.MkdirAll(fh.Path, 0755)
}

func (fh *FileHandler) OpenFile(remotePath *RemotePath) error {

	var err error
	if _, ok := fh.Cached[remotePath.String()]; !ok {

		// TODO Implement some form of cache management

		if fh.checkCacheSpace() {
			resp := fh.Ifs.Talker.sendRequest(FetchFileRequest, remotePath)

			if err, ok := resp.Data.(error); ok {
				return err
			}

			err = ioutil.WriteFile(path.Join(fh.Path, fh.convertToCacheName(remotePath)), resp.Data.(*FileChunk).Chunk,
				0666)
			fh.Cached[remotePath.String()] = true
		}

	}

	fh.Opened[remotePath.String()] = true

	return err
}

func (fh *FileHandler) checkCacheSpace() bool {
	// TODO Implement properly
	return fh.Size > 0
}

func (fh *FileHandler) convertToCacheName(path *RemotePath) string {
	s := strings.Replace(path.String(), "/", "_", -1)
	s = strings.Replace(s, ":", "_", 1)
	s = strings.Replace(s, "@", "_", 1)
	return s
}

// TODO Skip Cache if io op fails
func (fh *FileHandler) ReadData(remotePath *RemotePath, offset int64, size int) ([]byte, int, error) {

	// TODO  Check if File is Open
	if _, ok := fh.Cached[remotePath.String()]; ok {
		f, err := os.OpenFile(path.Join(fh.Path, fh.convertToCacheName(remotePath)), os.O_RDONLY, 0666)
		defer f.Close()

		// File is already opened
		if err != nil {
			log.Fatal(err)
		}

		b := make([]byte, size)
		n, err := f.ReadAt(b, offset)

		return b, n, err
	} else {
		// Should Ask Agent for bytes
		fileReadInfo := &ReadInfo{
			RemotePath: remotePath,
			Offset:     offset,
			Size:       size,
		}

		resp := fh.Ifs.Talker.sendRequest(ReadFileRequest, fileReadInfo)

		if err, ok := resp.Data.(error); ok {
			return nil, 0, err
		} else {
			fileChunk := resp.Data.(*FileChunk)
			return fileChunk.Chunk, fileChunk.Size, nil
		}

	}
}

func (fh *FileHandler) ReadAllData(remotePath *RemotePath) ([]byte, int, error) {
	if _, ok := fh.Cached[remotePath.String()]; ok {
		data, err := ioutil.ReadFile(path.Join(fh.Path, fh.convertToCacheName(remotePath)))
		return data, len(data), err
	} else {
		resp := fh.Ifs.Talker.sendRequest(FetchFileRequest, remotePath)

		if err, ok := resp.Data.(error); ok {
			return nil, 0, err
		} else {
			fileChunk := resp.Data.(*FileChunk)
			return fileChunk.Chunk, len(fileChunk.Chunk), nil
		}
	}
}

// TODO Parallize Cache and Remote Writes
func (fh *FileHandler) WriteData(remotePath *RemotePath, data []byte, offset int64) (int, error) {

	var err error
	if _, ok := fh.Cached[remotePath.String()]; ok {
		f, err := os.OpenFile(path.Join(fh.Path, fh.convertToCacheName(remotePath)), os.O_WRONLY, 0666)
		defer f.Close()

		// File is already opened
		if err != nil {
			log.Fatal(err)
		}

		_, err = f.WriteAt(data, offset)

	}

	if err == nil {

		// Send Bytes to Agent
		writeInfo := &WriteInfo{
			RemotePath: remotePath,
			Offset:     offset,
			Data:       data,
		}
		resp := fh.Ifs.Talker.sendRequest(WriteFileRequest, writeInfo)
		if err, ok := resp.Data.(error); ok {
			return 0, err
		} else {
			writeResult := resp.Data.(*WriteResult)
			return writeResult.Size, nil
		}

	}

	return 0, err

}

func (fh *FileHandler) Truncate(remotePath *RemotePath, size uint64) error {

	var err error
	if _, ok := fh.Cached[remotePath.String()]; ok {
		err = os.Truncate(path.Join(fh.Path, fh.convertToCacheName(remotePath)), int64(size))
	}

	if err == nil {

		attrInfo := &TruncInfo{
			RemotePath: remotePath,
			Size:       size,
		}

		resp := fh.Ifs.Talker.sendRequest(TruncateRequest, attrInfo)

		if err, ok := resp.Data.(error); ok {
			return err
		}
	}

	return err
}

func (fh *FileHandler) Release(remotePath *RemotePath) error {
	if _, ok := fh.Opened[remotePath.String()]; ok {
		delete(fh.Opened, remotePath.String())
	}
	return nil
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

	if fh.checkCacheSpace() {

		f, err := os.Create(path.Join(fh.Path, fh.convertToCacheName(newRemotePath)))
		defer f.Close()

		// Need to ignore
		if err != nil {
			log.Fatal(err)
		}

		fh.Cached[newRemotePath.String()] = true
	}

	fh.Opened[newRemotePath.String()] = true

	if err, ok := resp.Data.(error); ok {
		return err
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

	if err, ok := resp.Data.(error); ok {
		return err
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

	// TODO No Need to delete immediately, but can give to cache manager
	// TODO Remove from Cached map
	if _, ok := fh.Cached[remotePath.String()]; ok {
		err := os.Remove(path.Join(fh.Path, fh.convertToCacheName(newRemotePath)))

		if err != nil {
			log.Fatal(err)
		}

		delete(fh.Cached, remotePath.String())

	}

	if err, ok := resp.Data.(error); ok {
		return err
	}

	return nil
}
