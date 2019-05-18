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
// fetchFile is RemotePath
// Read From Cache is ReadInfo
// Write To Cache is WriteInfo
// SetAttr To Cache is AttrInfo
// Delete is RemotePath

type DiskCacheManager struct {
	Path     string
	Size     uint64
	Sender   communicator.Sender
	fileId   uint64
	fd       uint64
	opened   sync.Map
	cached   sync.Map
	fetching structures.MutexMap
}

func NewDiskCacheManager() *DiskCacheManager {
	return &DiskCacheManager{
		fileId:   0,
		fd:       0,
		opened:   sync.Map{},
		cached:   sync.Map{},
		fetching: structures.NewMutexMap(1000),
	}
}

//Private Methods
func (dcm *DiskCacheManager) isCached(rp *structures.RemotePath) bool {
	_, ok := dcm.cached.Load(rp.PrettyString())
	return ok
}

func (dcm *DiskCacheManager) deleteCache() {
	zap.L().Info("Deleting Cache")
	err := os.RemoveAll(dcm.Path)

	if err != nil {
		zap.L().Fatal("Failed to Delete Cache",
			zap.Error(err),
		)
	}

	err = os.MkdirAll(dcm.Path, 0755)

	if err != nil {
		zap.L().Fatal("Failed to Mkdir",
			zap.Error(err),
		)
	}

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

		reply, err := dcm.Sender.SendRequest(structures.FetchMessageCode, rp.Address(), payload)

		if err != nil {
			return err
		}

		fileMsg := reply.GetFileMsg()
		fname := dcm.getNextCacheFileName()

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
//TODO Change to take Config as input
func (dcm *DiskCacheManager) Run(path string, size uint64) {

	dcm.Path = path
	dcm.Size = size

	dcm.deleteCache()
}

func (dcm *DiskCacheManager) Rename(remotePath *structures.RemotePath, dst string) error {

	zap.L().Debug("Rename",
		zap.String("remote-remotePath", remotePath.PrettyString()),
		zap.String("dst", dst),
	)

	renameMsg := &structures.RenameMessage{
		CurrentPath: remotePath.Path,
		NewPath:     dst,
	}

	payload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_RenameMsg{
			RenameMsg: renameMsg,
		},
	}

	_, err := dcm.Sender.SendRequest(structures.RenameMessageCode, remotePath.Address(), payload)

	if err != nil {
		return err
	}

	if val, ok := dcm.cached.Load(remotePath.PrettyString()); ok {
		fname := val.(string)

		dstRemotePath := &structures.RemotePath{
			Hostname: remotePath.Hostname,
			Port:     remotePath.Port,
			Path:     dst,
		}

		dcm.cached.Store(dstRemotePath.PrettyString(), fname)
		dcm.cached.Delete(remotePath.PrettyString())

		return nil
	}

	return os.ErrInvalid
}

func (dcm *DiskCacheManager) Open(filePath *structures.RemotePath, flags int) (uint64, error) {

	zap.L().Debug("Open",
		zap.String("remote-path", filePath.PrettyString()),
		zap.Int("flags", flags),
	)

	fd := atomic.AddUint64(&dcm.fd, 1)

	openMsg := &structures.OpenMessage{
		Fd:    fd,
		Path:  filePath.Path,
		Flags: int32(flags),
	}

	payload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_OpenMsg{
			OpenMsg: openMsg,
		},
	}

	_, err := dcm.Sender.SendRequest(structures.OpenMessageCode, filePath.Address(), payload)

	if err != nil {
		return 0, err
	}

	var f *os.File
	if val, ok := dcm.cached.Load(filePath.PrettyString()); ok {

		f, err = os.OpenFile(path.Join(dcm.Path, val.(string)), flags, 0666)

		if err != nil {
			return 0, err
		}

	} else {

		err := dcm.fetch(filePath)

		if err != nil {
			return 0, err
		}

		val, _ := dcm.cached.Load(filePath.PrettyString())
		f, err = os.OpenFile(path.Join(dcm.Path, val.(string)), int(flags), 0666)

		if err != nil {
			return 0, err
		}

	}

	dcm.opened.Store(fd, f)

	return fd, err
}

func (dcm *DiskCacheManager) Create(dirPath *structures.RemotePath, name string) (uint64, error) {

	zap.L().Debug("Create",
		zap.String("base-dir", dirPath.PrettyString()),
		zap.String("name", name),
	)

	fd := atomic.AddUint64(&dcm.fd, 1)

	createMsg := &structures.CreateMessage{
		Fd:      fd,
		BaseDir: dirPath.Path,
		Name:    name,
	}

	payload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_CreateMsg{
			CreateMsg: createMsg,
		},
	}

	_, err := dcm.Sender.SendRequest(structures.CreateMessageCode, dirPath.Address(), payload)

	if err != nil {
		return 0, err
	}

	filePath := &structures.RemotePath{
		Hostname: dirPath.Hostname,
		Port:     dirPath.Port,
		Path:     path.Join(dirPath.Path, name),
	}

	fname := dcm.getNextCacheFileName()

	f, err := os.Create(path.Join(dcm.Path, fname))

	// TODO Remote and Local will be out of sync
	if err != nil {
		return 0, err
	}

	dcm.cached.Store(filePath.PrettyString(), fname)
	dcm.opened.Store(fd, f)

	return fd, nil
}

func (dcm *DiskCacheManager) Remove(filePath *structures.RemotePath) error {

	zap.L().Debug("Remove",
		zap.String("remote-path", filePath.PrettyString()),
	)

	removeMsg := &structures.RemoveMessage{
		Path: filePath.Path,
	}

	payload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_RemoveMsg{
			RemoveMsg: removeMsg,
		},
	}

	_, err := dcm.Sender.SendRequest(structures.RemoveMessageCode, filePath.Address(), payload)

	if err != nil {
		return err
	}

	// TODO Think About keeping stuff in sync
	if val, ok := dcm.cached.Load(filePath.PrettyString()); ok {

		fname := val.(string)

		err = os.Remove(path.Join(dcm.Path, fname))

		if err != nil {
			return err
		}

		dcm.cached.Delete(filePath.PrettyString())
	}

	return nil
}

func (dcm *DiskCacheManager) Close(filePath *structures.RemotePath, fd uint64) error {

	zap.L().Debug("Close",
		zap.String("remote-path", filePath.PrettyString()),
		zap.Uint64("fd", fd),
	)

	closeMsg := &structures.CloseMessage{
		Fd: fd,
	}

	payload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_CloseMsg{
			CloseMsg: closeMsg,
		},
	}

	_, err := dcm.Sender.SendRequest(structures.CloseMessageCode, filePath.Address(), payload)

	if err != nil {
		return err
	}

	if val, ok := dcm.opened.Load(fd); ok {
		f := val.(*os.File)
		err = f.Close()
		dcm.opened.Delete(fd)
		return err
	}

	return os.ErrInvalid
}

func (dcm *DiskCacheManager) Truncate(filePath *structures.RemotePath, size uint64) error {

	zap.L().Debug("Truncate",
		zap.String("remote-path", filePath.PrettyString()),
		zap.Uint64("size", size),
	)

	truncateMessage := &structures.TruncateMessage{
		Path: filePath.Path,
		Size: size,
	}

	payload := &structures.RequestPayload{
		Payload: &structures.RequestPayload_TruncateMsg{
			TruncateMsg: truncateMessage,
		},
	}

	_, err := dcm.Sender.SendRequest(structures.TruncateMessageCode, filePath.Address(), payload)

	if err != nil {
		return err
	}


	if val, ok := dcm.cached.Load(filePath.PrettyString()); ok {
		err := os.Truncate(path.Join(dcm.Path, val.(string)), int64(size))

		if err != nil {
			return err
		}
	}

	return nil
}


//func (h *hoarder) SendWrite(hostname string, writeInfo *WriteInfo) error {
//	// TODO Log the error if any ?
//	Talker().sendRequest(WriteFileRequest, hostname, writeInfo)
//	return nil
//}
//
//

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

//func (h *Hoarder) CacheFlush(fd uint64) error {
//	if val, ok := h.opened.Load(fd); ok {
//		f := val.(*os.File)
//		return f.Sync()
//	}
//
//	return os.ErrInvalid
//}
