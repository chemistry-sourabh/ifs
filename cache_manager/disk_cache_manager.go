/*
 * Copyright 2019 Sourabh Bollapragada
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cache_manager

import (
	"github.com/chemistry-sourabh/ifs/communicator"
	"github.com/chemistry-sourabh/ifs/structures"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
)

// Use Packet
// FetchFile is RemotePath
// Read From Cache is ReadInfo
// Write To Cache is WriteInfo
// SetAttr To Cache is AttrInfo
// Delete is RemotePath

type DiskCacheManager struct {
	Path     string
	Size     uint64
	Nm       communicator.Sender
	fileId   uint64
	cached   sync.Map
	fetching structures.MutexMap
}

func NewDiskCacheManager() *DiskCacheManager {
	return &DiskCacheManager{
		fileId:   1,
		cached:   sync.Map{},
		fetching: structures.NewMutexMap(1000),
	}
}

//Private Methods
func (dcm *DiskCacheManager) isCached(rp *structures.RemotePath) bool {
	_, ok := dcm.cached.Load(rp.PrettyString())
	return ok
}

func (dcm *DiskCacheManager) deleteCache() error {
	zap.L().Info("Deleting Cache")
	err := os.RemoveAll(dcm.Path)

	if err != nil {
		return err
	}

	err = os.MkdirAll(dcm.Path, 0755)
	return err
}

func (dcm *DiskCacheManager) getNextCacheFileName() string {
	fileId := atomic.AddUint64(&dcm.fileId, 1)
	return strconv.FormatUint(fileId, 10)
}

func (dcm *DiskCacheManager) fetch(rp *structures.RemotePath) error {

	dcm.fetching.Lock(rp.PrettyString())
	defer dcm.fetching.Unlock(rp.PrettyString())

	if !dcm.isCached(rp) {

		fetchMsg := &structures.FetchMessage{
			Path: rp.Path,
		}

		payload := &structures.RequestPayload{
			Payload: &structures.RequestPayload_FetchMsg{
				FetchMsg: fetchMsg,
			},
		}

		msg, err := dcm.Nm.SendRequest(structures.FetchMessageCode, rp.Address(), payload)

		if err != nil {
			return err
		}

		fname := dcm.getNextCacheFileName()
		fileMsg := msg.GetFileMsg()

		err = ioutil.WriteFile(path.Join(dcm.Path, fname), fileMsg.File, 0666)

		if err != nil {
			return err
		}

		val, ok := dcm.cached.Load(rp.PrettyString())
		dcm.cached.Store(rp.PrettyString(), fname)
		if ok {
			oldFname := val.(string)
			err = os.Remove(path.Join(dcm.Path, oldFname))

			if err != nil {
				return err
			}

		}

	}

	return nil
}

// Interface Methods
func (dcm *DiskCacheManager) Run(path string, size uint64) {

	dcm.Path = path
	dcm.Size = size

	dcm.deleteCache()
}

func (dcm *DiskCacheManager) Rename(path *structures.RemotePath, dst string) error {
	if val, ok := dcm.cached.Load(path.PrettyString()); ok {
		fname := val.(string)

		dstRemotePath := &structures.RemotePath{
			Hostname: path.Hostname,
			Port:     path.Port,
			Path:     dst,
		}

		dcm.cached.Store(dstRemotePath.PrettyString(), fname)
		dcm.cached.Delete(path.PrettyString())

		return nil
	}

	return os.ErrInvalid
}

func (dcm *DiskCacheManager) Open(filePath *structures.RemotePath, flags int) (*os.File, error) {

	if val, ok := dcm.cached.Load(filePath.PrettyString()); ok {

		f, err := os.OpenFile(path.Join(dcm.Path, val.(string)), flags, 0666)

		if err != nil {
			return nil, err
		}

		return f, err
	} else {

		err := dcm.fetch(filePath)

		if err != nil {
			return nil, err
		}

		val, _ := dcm.cached.Load(filePath.PrettyString())
		f, err := os.OpenFile(path.Join(dcm.Path, val.(string)), int(flags), 0666)

		if err != nil {
			return nil, err
		}

		return f, err
	}
}

//func (h *hoarder) SendWrite(hostname string, writeInfo *WriteInfo) error {
//	// TODO Log the error if any ?
//	Talker().sendRequest(WriteFileRequest, hostname, writeInfo)
//	return nil
//}
//
//func (h *hoarder) CacheTrunc(remotePath *RemotePath, truncInfo *AttrInfo) error {
//	if fname, ok := h.cached.Get(remotePath.String()); ok {
//		err := os.Truncate(path.Join(h.Path, fname.(string)), int64(truncInfo.Size))
//		return err
//	}
//
//	return os.ErrNotExist
//}
//
//func (h *hoarder) CacheCreate(remotePath *RemotePath, fd uint64) error {
//	if !h.isCached(remotePath) {
//		fname := h.GetCacheFileName()
//		f, err := os.Create(path.Join(h.Path, fname))
//
//		// if error doesnt happens this will be nil right ?
//		if err == nil {
//			h.cached.Set(remotePath.String(), fname)
//			h.opened.Set(strconv.FormatUint(fd, 10), f)
//		}
//
//		return err
//	}
//
//	return os.ErrExist
//}
//
//func (h *hoarder) CacheDelete(remotePath *RemotePath) error {
//	if val, ok := h.cached.Get(remotePath.String()); ok {
//
//		fname := val.(string)
//
//		err := os.Remove(path.Join(h.Path, fname))
//
//		if err == nil {
//			h.cached.Remove(remotePath.String())
//		}
//
//		return err
//	}
//
//	return os.ErrInvalid
//}
//
////func (h *Hoarder) ReadAllCache(remotePath *RemotePath) ([]byte, error) {
////	if fname, ok := h.cached[remotePath.String()]; ok {
////		data, err := ioutil.ReadFile(path.Join(h.Path, fname))
////		return data, err
////	}
////
////	return nil, os.ErrNotExist
////}
//
//func (h *hoarder) ReadCache(fd uint64, offset int64, size int) ([]byte, error) {
//	if val, ok := h.opened.Get(strconv.FormatUint(fd, 10)); ok {
//
//		f := val.(*os.File)
//
//		b := make([]byte, size)
//		_, err := f.ReadAt(b, offset)
//
//		if err == nil {
//			return b, nil
//		}
//
//		return nil, err
//	}
//
//	return nil, os.ErrInvalid
//}
//
//func (h *hoarder) WriteCache(fd uint64, offset int64, data []byte) (int, error) {
//	if val, ok := h.opened.Get(strconv.FormatUint(fd, 10)); ok {
//		f := val.(*os.File)
//		n, err := f.WriteAt(data, offset)
//		return n, err
//	}
//
//	return 0, os.ErrInvalid
//}
//
//func (h *hoarder) CacheClose(fd uint64) error {
//	if val, ok := h.opened.Get(strconv.FormatUint(fd, 10)); ok {
//		f := val.(*os.File)
//		return f.Close()
//	}
//
//	return os.ErrInvalid
//}

//func (h *Hoarder) CacheFlush(fd uint64) error {
//	if val, ok := h.opened.Load(fd); ok {
//		f := val.(*os.File)
//		return f.Sync()
//	}
//
//	return os.ErrInvalid
//}
