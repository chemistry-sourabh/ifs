package ifs

import (
	"io/ioutil"
	"path"
	"fmt"
	"os"
	"sync"
	"go.uber.org/zap"
	"bazil.org/fuse"
)

type CacheRequest interface {
}

// Use Packet
// FetchFile is RemotePath
// Read From Cache is ReadInfo
// Write To Cache is WriteInfo
// SetAttr To Cache is AttrInfo
// Delete is RemotePath

type Hoarder struct {
	Ifs    *Ifs
	Path   string
	Size   uint64
	cached *sync.Map
	//cached   map[string]string
	fetching *sync.Map
	//fetching       map[string]bool
	opened *sync.Map
	//openedFiles    map[uint64]*os.File
	fetchQueue     chan *FetchInfo
	fileId         uint
	fileDescriptor uint64
}

func (h *Hoarder) Startup() {
	h.fetching = &sync.Map{}
	h.cached = &sync.Map{}
	h.opened = &sync.Map{}
	h.fetchQueue = make(chan *FetchInfo, ChannelLength)
	h.fileId = 0

	h.DeleteCache()

	go h.processFetchRequests()
}

func (h *Hoarder) DeleteCache() {
	zap.L().Info("Deleting Cache")
	os.RemoveAll(h.Path)
	os.MkdirAll(h.Path, 0755)
}

//func (h *Hoarder) ProcessCacheRequests() {
//
//	for pkt := range h.ingress {
//
//		switch pkt.Op {
//		case CacheFileRequest:
//			rp := pkt.Data.(*RemotePath)
//			h.cacheFile(rp)
//		case CacheWriteRequest:
//			writeInfo := pkt.Data.(*WriteInfo)
//			h.SendWrite(writeInfo)
//		case CacheTruncRequest:
//			truncInfo := pkt.Data.(*AttrInfo)
//			h.CacheTrunc(truncInfo)
//		case CacheCreateRequest:
//			rp := pkt.Data.(*RemotePath)
//			h.CacheCreate(rp)
//		case CacheDeleteRequest:
//			rp := pkt.Data.(*RemotePath)
//			h.CacheDelete(rp)
//		case CacheRenameRequest:
//			req := pkt.Data.(*RenameInfo)
//			h.CacheRename(req.RemotePath, req.DestPath)
//		case CacheOpenRequest:
//			req := pkt.Data.(*OpenInfo)
//			h.CacheOpen(req.RemotePath, req.FileDescriptor, req.Flags)
//		}
//
//	}
//
//}

//func (h *Hoarder) SubmitRequest(opCode uint8, payload Payload) {
//	req := &Packet{
//		Op:   opCode,
//		Data: payload,
//	}
//
//	h.ingress <- req
//}

func (h *Hoarder) CacheRename(remotePath *RemotePath, destPath string) {
	if val, ok := h.cached.Load(remotePath.String()); ok {

		fname := val.(string)

		newRemotePath := &RemotePath{
			Hostname: remotePath.Hostname,
			Port:     remotePath.Port,
			Path:     destPath,
		}

		h.cached.Store(newRemotePath.String(), fname)
		h.cached.Delete(remotePath.String())
	}
}

func (h *Hoarder) IsCached(rp *RemotePath) bool {
	_, ok := h.cached.Load(rp.String())
	return ok
}

func (h *Hoarder) openCacheFile(fname string, fileDescriptor uint64, flags fuse.OpenFlags) error {

	f, err := os.OpenFile(path.Join(h.Path, fname), int(flags), 0666)

	if err != nil {
		return err
	}

	h.opened.Store(fileDescriptor, f)
	return nil
}

func (h *Hoarder) CacheOpen(remotePath *RemotePath, fileDescriptor uint64, flags fuse.OpenFlags) {

	if val, ok := h.cached.Load(remotePath.String()); ok {
		h.openCacheFile(val.(string), fileDescriptor, flags)
	} else {

		fetchInfo := &FetchInfo{
			RemotePath:     remotePath,
			FileDescriptor: fileDescriptor,
			Flags:          flags,
		}

		h.fetchQueue <- fetchInfo
	}
}

func (h *Hoarder) processFetchRequests() {
	for openInfo := range h.fetchQueue {

		rp := openInfo.RemotePath

		_, cachedOk := h.cached.Load(rp.String())
		_, fetchingOk := h.fetching.Load(rp.String())

		if !cachedOk && !fetchingOk {
			go func() {
				err := h.cacheFile(rp)

				if err == nil {
					val, _ := h.cached.Load(rp.String())
					h.openCacheFile(val.(string), openInfo.FileDescriptor, openInfo.Flags)
				}

			}()
		}

	}
}

func (h *Hoarder) cacheFile(remotePath *RemotePath) error {

	// TODO Check Cache Space
	// TODO Implement some form of cache management
	h.fetching.Store(remotePath.String(), true)

	resp := h.Ifs.Talker.sendRequest(FetchFileRequest, remotePath.Hostname, remotePath)

	// TODO Log Error
	if err, ok := resp.Data.(Error); ok {
		return err.Err
	}

	fname := h.GetCacheFileName()
	fileChunk := resp.Data.(*FileChunk)
	fileChunk.Decompress()
	err := ioutil.WriteFile(path.Join(h.Path, fname), fileChunk.Chunk,
		0666)

	if err == nil {
		h.cached.Store(remotePath.String(), fname)
	}
	h.fetching.Delete(remotePath.String())

	return err
}

func (h *Hoarder) SendWrite(hostname string, writeInfo *WriteInfo) error {
	// TODO Log the error if any ?
	h.Ifs.Talker.sendRequest(WriteFileRequest, hostname, writeInfo)
	return nil
}

func (h *Hoarder) CacheTrunc(remotePath *RemotePath, truncInfo *AttrInfo) error {
	if fname, ok := h.cached.Load(remotePath.String()); ok {
		err := os.Truncate(path.Join(h.Path, fname.(string)), int64(truncInfo.Size))
		return err
	}

	return os.ErrNotExist
}

func (h *Hoarder) CacheCreate(remotePath *RemotePath, fd uint64) error {
	if _, ok := h.cached.Load(remotePath.String()); !ok {
		fname := h.GetCacheFileName()
		f, err := os.Create(path.Join(h.Path, fname))

		// if error doesnt happens this will be nil right ?
		if err == nil {
			h.cached.Store(remotePath.String(), fname)
			h.opened.Store(fd, f)
		}

		return err
	}

	return os.ErrExist
}

func (h *Hoarder) CacheDelete(remotePath *RemotePath) error {
	if val, ok := h.cached.Load(remotePath.String()); ok {

		fname := val.(string)

		err := os.Remove(path.Join(h.Path, fname))

		if err == nil {
			h.cached.Delete(remotePath.String())
		}

		return err
	}

	return os.ErrInvalid
}

//func (h *Hoarder) ReadAllCache(remotePath *RemotePath) ([]byte, error) {
//	if fname, ok := h.cached[remotePath.String()]; ok {
//		data, err := ioutil.ReadFile(path.Join(h.Path, fname))
//		return data, err
//	}
//
//	return nil, os.ErrNotExist
//}

func (h *Hoarder) ReadCache(fd uint64, offset int64, size int) ([]byte, error) {
	if val, ok := h.opened.Load(fd); ok {

		f := val.(*os.File)

		b := make([]byte, size)
		_, err := f.ReadAt(b, offset)


		if err == nil {
			return b, nil
		}

		return nil, err
	}

	return nil, os.ErrInvalid
}

func (h *Hoarder) GetCacheFileName() string {
	fileId := h.fileId
	h.fileId++
	return fmt.Sprintf("%d", fileId)
}

func (h *Hoarder) WriteCache(fd uint64, offset int64, data []byte) (int, error) {
	if val, ok := h.opened.Load(fd); ok {
		f := val.(*os.File)
		n, err := f.WriteAt(data, offset)
		return n, err
	}

	return 0, os.ErrInvalid
}

func (h *Hoarder) CacheClose(fd uint64) error {
	if val, ok := h.opened.Load(fd); ok {
		f := val.(*os.File)
		return f.Close()
	}

	return os.ErrInvalid
}

//func (h *Hoarder) CacheFlush(fd uint64) error {
//	if val, ok := h.opened.Load(fd); ok {
//		f := val.(*os.File)
//		return f.Sync()
//	}
//
//	return os.ErrInvalid
//}