package ifs

import (
	"io/ioutil"
	"path"
	"fmt"
	"os"
	log "github.com/sirupsen/logrus"
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
	Ifs      *Ifs
	Path     string
	Size     uint64
	cached   map[string]string
	fetching map[string]bool
	ingress  chan *Packet
	fileId   uint
}

func (h *Hoarder) Startup() {
	h.fetching = make(map[string]bool)
	h.cached = make(map[string]string)
	h.ingress = make(chan *Packet, ChannelLength)
	h.fileId = 0

	h.DeleteCache()

	go h.ProcessCacheRequests()
}

func (h *Hoarder) DeleteCache() {
	log.Info("Deleting Cache")
	os.RemoveAll(h.Path)
	os.MkdirAll(h.Path, 0755)
}

func (h *Hoarder) ProcessCacheRequests() {

	for pkt := range h.ingress {

		switch pkt.Op {
		case CacheFileRequest:
			rp := pkt.Data.(*RemotePath)
			h.CacheFile(rp)
		case CacheWriteRequest:
			writeInfo := pkt.Data.(*WriteInfo)
			h.SendWrite(writeInfo)
		case CacheTruncRequest:
			truncInfo := pkt.Data.(*AttrInfo)
			h.CacheTrunc(truncInfo)
		case CacheCreateRequest:
			rp := pkt.Data.(*RemotePath)
			h.CacheCreate(rp)
		case CacheDeleteRequest:
			rp := pkt.Data.(*RemotePath)
			h.CacheDelete(rp)
		case CacheRenameRequest:
			req := pkt.Data.(*RenameInfo)
			h.CacheRename(req.RemotePath, req.DestPath)
		}

	}

}

func (h *Hoarder) SubmitRequest(opCode uint8, payload Payload) {
	req := &Packet{
		Op:   opCode,
		Data: payload,
	}

	h.ingress <- req
}

func (h *Hoarder) CacheRename(remotePath *RemotePath, destPath string) {
	if fname, ok := h.cached[remotePath.String()]; ok {

		newRemotePath := &RemotePath{
			Hostname: remotePath.Hostname,
			Port: remotePath.Port,
			Path: destPath,
		}

		h.cached[newRemotePath.String()] = fname
		delete(h.cached, remotePath.String())
	}
}

func (h *Hoarder) IsCached(rp *RemotePath) bool {
	_, ok := h.cached[rp.String()]
	return ok
}

func (h *Hoarder) CacheFile(remotePath *RemotePath) error {

	_, cachedOk := h.cached[remotePath.String()]
	_, fetchingOk := h.fetching[remotePath.String()]

	// TODO Check Cache Space
	// TODO Implement some form of cache management
	if !cachedOk && !fetchingOk{

		h.fetching[remotePath.String()] = true

		resp := h.Ifs.Talker.sendRequest(FetchFileRequest, remotePath)

		if err, ok := resp.Data.(Error); ok {
			return err.Err
		}

		fname := h.GetCacheFileName()
		fileChunk := resp.Data.(*FileChunk)
		fileChunk.Decompress()
		err := ioutil.WriteFile(path.Join(h.Path, fname), fileChunk.Chunk,
			0666)

		if err == nil {
			h.cached[remotePath.String()] = fname
			delete(h.fetching, remotePath.String())
		}
		return err
	}

	return os.ErrExist
}

func (h *Hoarder) SendWrite(writeInfo *WriteInfo) error {
	// TODO Log the error if any ?
	h.Ifs.Talker.sendRequest(WriteFileRequest, writeInfo)
	return nil
}

func (h *Hoarder) CacheTrunc(truncInfo *AttrInfo) error {
	if fname, ok := h.cached[truncInfo.RemotePath.String()]; ok {
		err := os.Truncate(path.Join(h.Path, fname), int64(truncInfo.Size))
		return err
	}

	return os.ErrNotExist
}

func (h *Hoarder) CacheCreate(remotePath *RemotePath) error {
	if _, ok := h.cached[remotePath.String()]; !ok {
		fname := h.GetCacheFileName()
		f, err := os.Create(path.Join(h.Path, fname))

		// if error doesnt happens this will be nil right ?
		if err == nil {
			defer f.Close()
			h.cached[remotePath.String()] = fname
		}

		return err
	}

	return os.ErrExist
}

func (h *Hoarder) CacheDelete(remotePath *RemotePath) error {
	if fname, ok := h.cached[remotePath.String()]; ok {
		err := os.Remove(path.Join(h.Path, fname))

		if err == nil {
			delete(h.cached, remotePath.String())
		}

		return err
	}

	return os.ErrNotExist
}

func (h *Hoarder) ReadAllCache(remotePath *RemotePath) ([]byte, error) {
	if fname, ok := h.cached[remotePath.String()]; ok {
		data, err := ioutil.ReadFile(path.Join(h.Path, fname))
		return data, err
	}

	return nil, os.ErrNotExist
}

func (h *Hoarder) ReadCache(remotePath *RemotePath, offset int64, size int) ([]byte, error) {
	if fname, ok := h.cached[remotePath.String()]; ok {

		f, err := os.OpenFile(path.Join(h.Path, fname), os.O_RDONLY, 0666)
		defer f.Close()

		// Something is not right
		if err != nil {
			log.Panic(err)
		}

		b := make([]byte, size)
		_, err = f.ReadAt(b, offset)

		if err == nil {
			return b, nil
		}

		return nil, err
	}

	return nil, os.ErrNotExist
}

func (h *Hoarder) GetCacheFileName() string {
	fileId := h.fileId
	h.fileId++
	return fmt.Sprintf("%d", fileId)
}

func (h *Hoarder) WriteCache(remotePath *RemotePath, offset int64, data []byte) (int, error) {
	log.Println(h.cached)
	if fname, ok := h.cached[remotePath.String()]; ok {
		f, err := os.OpenFile(path.Join(h.Path, fname), os.O_WRONLY, 0666)

		// Something is not right
		if err != nil {
			log.Panic(err)
		} else {
			defer f.Close()
		}

		n, err := f.WriteAt(data, offset)

		write := &WriteInfo{
			RemotePath: remotePath,
			Offset:     offset,
			Data:       data,
		}

		h.SubmitRequest(CacheWriteRequest, write)

		return n, err
	}

	return 0, os.ErrNotExist
}
