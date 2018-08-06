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
	"io/ioutil"
	"path"
	"os"
	"sync"
	"go.uber.org/zap"
	"bazil.org/fuse"
	"sync/atomic"
	"strconv"
	"github.com/orcaman/concurrent-map"
)

type CacheRequest interface {
}

// Use Packet
// FetchFile is RemotePath
// Read From Cache is ReadInfo
// Write To Cache is WriteInfo
// SetAttr To Cache is AttrInfo
// Delete is RemotePath

type hoarder struct {
	Path   string
	Size   uint64
	cached cmap.ConcurrentMap
	//fetching   cmap.ConcurrentMap
	fetching   MutexMap
	opened     cmap.ConcurrentMap
	fetchQueue chan interface{}
	fileId     uint64
}

var (
	hoarderInstance *hoarder
	hoarderOnce     sync.Once
)

func Hoarder() *hoarder {
	hoarderOnce.Do(func() {
		hoarderInstance = &hoarder{
			fileId:     1,
			cached:     cmap.New(),
			fetching:   *NewMutexMap(),
			opened:     cmap.New(),
			fetchQueue: make(chan interface{}, ChannelLength),
		}
	})

	return hoarderInstance
}

func (h *hoarder) Startup(path string, size uint64) {

	h.Path = path
	h.Size = size

	h.DeleteCache()

	//go h.processFetchRequests()
}

func (h *hoarder) DeleteCache() {
	zap.L().Info("Deleting Cache")
	os.RemoveAll(h.Path)
	os.MkdirAll(h.Path, 0755)
}

func (h *hoarder) CacheRename(remotePath *RemotePath, destPath string) error {
	if val, ok := h.cached.Get(remotePath.String()); ok {

		fname := val.(string)

		newRemotePath := &RemotePath{
			Hostname: remotePath.Hostname,
			Port:     remotePath.Port,
			Path:     destPath,
		}

		h.cached.Set(newRemotePath.String(), fname)
		h.cached.Remove(remotePath.String())

		return nil
	}

	return os.ErrInvalid
}

func (h *hoarder) IsCached(rp *RemotePath) bool {
	_, ok := h.cached.Get(rp.String())
	return ok
}

func (h *hoarder) openCacheFile(fname string, fileDescriptor uint64, flags fuse.OpenFlags) error {

	f, err := os.OpenFile(path.Join(h.Path, fname), int(flags), 0666)

	if err != nil {
		return err
	}

	h.opened.Set(strconv.FormatUint(fileDescriptor, 10), f)
	return nil
}

func (h *hoarder) CacheOpen(remotePath *RemotePath, fileDescriptor uint64, flags fuse.OpenFlags) {

	if val, ok := h.cached.Get(remotePath.String()); ok {
		h.openCacheFile(val.(string), fileDescriptor, flags)
	} else {

		fetchInfo := &FetchInfo{
			RemotePath:     remotePath,
			FileDescriptor: fileDescriptor,
			Flags:          flags,
		}

		h.cacheAndOpen(fetchInfo)
	}
}

func (h *hoarder) cacheAndOpen(info *FetchInfo) error {

	zap.L().Debug("Cache File",
		zap.String("remotePath", info.RemotePath.String()),
	)

	h.fetching.Lock(info.RemotePath.String())
	defer h.fetching.Unlock(info.RemotePath.String())

	if !h.IsCached(info.RemotePath) {
		err := h.cacheFile(info.RemotePath)
		if err != nil {
			return err
		}
	}

	val, _ := h.cached.Get(info.RemotePath.String())

	return h.openCacheFile(val.(string), info.FileDescriptor, info.Flags)

}

func (h *hoarder) CacheFetch(remotePath *RemotePath) {

	zap.L().Debug("Fetch File",
		zap.String("remotePath", remotePath.String()),
	)

	h.fetching.Lock(remotePath.String())
	defer h.fetching.Unlock(remotePath.String())

	if h.IsCached(remotePath) {
		go h.cacheFile(remotePath)
	}
}

//func (h *hoarder) processFetchRequests() {
//	for info := range h.fetchQueue {
//
//		switch val := info.(type) {
//		case *FetchInfo:
//
//			fetchInfo := val
//
//			rp := fetchInfo.RemotePath
//
//			_, cachedOk := h.cached.Get(rp.String())
//			_, fetchingOk := h.fetching.Get(rp.String())
//
//			if !cachedOk && !fetchingOk {
//				go func() {
//					err := h.cacheFile(rp)
//
//					if err == nil {
//						val, _ := h.cached.Get(rp.String())
//						h.openCacheFile(val.(string), fetchInfo.FileDescriptor, fetchInfo.Flags)
//					}
//
//				}()
//			}
//
//		case *RemotePath:
//
//			rp := val
//
//			_, cachedOk := h.cached.Get(rp.String())
//			_, fetchingOk := h.fetching.Get(rp.String())
//
//			if cachedOk && !fetchingOk {
//				go h.cacheFile(rp)
//			}
//		}
//	}
//}

func (h *hoarder) cacheFile(remotePath *RemotePath) error {

	// TODO Check Cache Space
	// TODO Implement some form of cache management

	resp := Talker().sendRequest(FetchFileRequest, remotePath.Hostname, remotePath)

	// TODO Log Error
	if err, ok := resp.Data.(Error); ok {
		return err.Err
	}

	fname := h.GetCacheFileName()
	fileChunk := resp.Data.(*FileChunk)
	//fileChunk.Decompress()
	err := ioutil.WriteFile(path.Join(h.Path, fname), fileChunk.Chunk,
		0666)

	if err == nil {
		val, ok := h.cached.Get(remotePath.String())
		h.cached.Set(remotePath.String(), fname)
		if ok {
			oldFname := val.(string)
			os.Remove(path.Join(h.Path, oldFname))
		}
	}

	return err
}

func (h *hoarder) SendWrite(hostname string, writeInfo *WriteInfo) error {
	// TODO Log the error if any ?
	Talker().sendRequest(WriteFileRequest, hostname, writeInfo)
	return nil
}

func (h *hoarder) CacheTrunc(remotePath *RemotePath, truncInfo *AttrInfo) error {
	if fname, ok := h.cached.Get(remotePath.String()); ok {
		err := os.Truncate(path.Join(h.Path, fname.(string)), int64(truncInfo.Size))
		return err
	}

	return os.ErrNotExist
}

func (h *hoarder) CacheCreate(remotePath *RemotePath, fd uint64) error {
	if !h.IsCached(remotePath) {
		fname := h.GetCacheFileName()
		f, err := os.Create(path.Join(h.Path, fname))

		// if error doesnt happens this will be nil right ?
		if err == nil {
			h.cached.Set(remotePath.String(), fname)
			h.opened.Set(strconv.FormatUint(fd, 10), f)
		}

		return err
	}

	return os.ErrExist
}

func (h *hoarder) CacheDelete(remotePath *RemotePath) error {
	if val, ok := h.cached.Get(remotePath.String()); ok {

		fname := val.(string)

		err := os.Remove(path.Join(h.Path, fname))

		if err == nil {
			h.cached.Remove(remotePath.String())
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

func (h *hoarder) ReadCache(fd uint64, offset int64, size int) ([]byte, error) {
	if val, ok := h.opened.Get(strconv.FormatUint(fd, 10)); ok {

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

func (h *hoarder) GetCacheFileName() string {
	fileId := atomic.AddUint64(&h.fileId, 1)
	return strconv.FormatUint(fileId, 10)
}

func (h *hoarder) WriteCache(fd uint64, offset int64, data []byte) (int, error) {
	if val, ok := h.opened.Get(strconv.FormatUint(fd, 10)); ok {
		f := val.(*os.File)
		n, err := f.WriteAt(data, offset)
		return n, err
	}

	return 0, os.ErrInvalid
}

func (h *hoarder) CacheClose(fd uint64) error {
	if val, ok := h.opened.Get(strconv.FormatUint(fd, 10)); ok {
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
