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
	"github.com/chemistry-sourabh/ifs/structure"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
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
	fetching structure.MutexMap
}

func NewDiskCacheManager() *DiskCacheManager {
	return &DiskCacheManager{
		fileId:   0,
		fd:       0,
		opened:   sync.Map{},
		cached:   sync.Map{},
		fetching: structure.NewMutexMap(1000),
	}
}

//Private Methods
func (dcm *DiskCacheManager) isCached(rp *structure.RemotePath) bool {
	_, ok := dcm.cached.Load(rp.String())
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

func (dcm *DiskCacheManager) fetch(rp *structure.RemotePath) error {

	dcm.fetching.Lock(rp.String())
	defer dcm.fetching.Unlock(rp.String())

	if !dcm.isCached(rp) {

		fetchMsg := &structure.FetchMessage{
			Path: rp.Path,
		}

		payload := &structure.RequestPayload{
			Payload: &structure.RequestPayload_FetchMsg{
				FetchMsg: fetchMsg,
			},
		}

		reply, err := dcm.Sender.SendRequest(structure.FetchMessageCode, rp.Address(), payload)

		if err != nil {
			return err
		}

		dataMsg := reply.GetDataMsg()
		fname := dcm.getNextCacheFileName()

		err = ioutil.WriteFile(path.Join(dcm.Path, fname), dataMsg.GetData(), 0666)

		if err != nil {
			return err
		}

		val, ok := dcm.cached.Load(rp.String())
		dcm.cached.Store(rp.String(), fname)
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

func (dcm *DiskCacheManager) Rename(remotePath *structure.RemotePath, dst string) error {

	zap.L().Debug("Rename",
		zap.String("remote-remotePath", remotePath.String()),
		zap.String("dst", dst),
	)

	renameMsg := &structure.RenameMessage{
		CurrentPath: remotePath.Path,
		NewPath:     dst,
	}

	payload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_RenameMsg{
			RenameMsg: renameMsg,
		},
	}

	_, err := dcm.Sender.SendRequest(structure.RenameMessageCode, remotePath.Address(), payload)

	if err != nil {
		return err
	}

	if val, ok := dcm.cached.Load(remotePath.String()); ok {
		fname := val.(string)

		dstRemotePath := &structure.RemotePath{
			Hostname: remotePath.Hostname,
			Port:     remotePath.Port,
			Path:     dst,
		}

		dcm.cached.Store(dstRemotePath.String(), fname)
		dcm.cached.Delete(remotePath.String())

	}

	return nil
}

func (dcm *DiskCacheManager) Open(filePath *structure.RemotePath, flags uint32) (uint64, error) {

	zap.L().Debug("Open",
		zap.String("remote-path", filePath.String()),
		zap.Uint32("flags", flags),
	)

	fd := atomic.AddUint64(&dcm.fd, 1)

	openMsg := &structure.OpenMessage{
		Fd:    fd,
		Path:  filePath.Path,
		Flags: flags,
	}

	payload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_OpenMsg{
			OpenMsg: openMsg,
		},
	}

	_, err := dcm.Sender.SendRequest(structure.OpenMessageCode, filePath.Address(), payload)

	if err != nil {
		return 0, err
	}

	var f *os.File
	if val, ok := dcm.cached.Load(filePath.String()); ok {

		f, err = os.OpenFile(path.Join(dcm.Path, val.(string)), int(flags), 0666)

		if err != nil {
			return 0, err
		}

	} else {

		err := dcm.fetch(filePath)

		if err != nil {
			return 0, err
		}

		val, _ := dcm.cached.Load(filePath.String())
		f, err = os.OpenFile(path.Join(dcm.Path, val.(string)), int(flags), 0666)

		if err != nil {
			return 0, err
		}

	}

	dcm.opened.Store(fd, &structure.CacheFileHandle{
		FilePath: filePath,
		Fp:       f,
	})

	return fd, err
}

func (dcm *DiskCacheManager) Create(dirPath *structure.RemotePath, name string) (uint64, error) {

	zap.L().Debug("Create",
		zap.String("base-dir", dirPath.String()),
		zap.String("name", name),
	)

	fd := atomic.AddUint64(&dcm.fd, 1)

	createMsg := &structure.CreateMessage{
		Fd:      fd,
		BaseDir: dirPath.Path,
		Name:    name,
	}

	payload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_CreateMsg{
			CreateMsg: createMsg,
		},
	}

	_, err := dcm.Sender.SendRequest(structure.CreateMessageCode, dirPath.Address(), payload)

	if err != nil {
		return 0, err
	}

	filePath := &structure.RemotePath{
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

	dcm.cached.Store(filePath.String(), fname)
	dcm.opened.Store(fd, &structure.CacheFileHandle{
		FilePath: filePath,
		Fp:       f,
	})

	return fd, nil
}

func (dcm *DiskCacheManager) Mkdir(dirPath *structure.RemotePath, name string) error {
	zap.L().Debug("Mkdir",
		zap.String("base-dir", dirPath.String()),
		zap.String("name", name),
	)

	mkdirMsg := &structure.MkdirMessage{
		BaseDir: dirPath.Path,
		Name:    name,
	}

	payload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_MkdirMsg{
			MkdirMsg: mkdirMsg,
		},
	}

	_, err := dcm.Sender.SendRequest(structure.MkdirMessageCode, dirPath.Address(), payload)

	if err != nil {
		return err
	}

	return nil
}

func (dcm *DiskCacheManager) Remove(filePath *structure.RemotePath, isDir bool) error {

	zap.L().Debug("Remove",
		zap.String("remote-path", filePath.String()),
		zap.Bool("is-dir", isDir),
	)

	removeMsg := &structure.RemoveMessage{
		Path:  filePath.Path,
		IsDir: isDir,
	}

	payload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_RemoveMsg{
			RemoveMsg: removeMsg,
		},
	}

	_, err := dcm.Sender.SendRequest(structure.RemoveMessageCode, filePath.Address(), payload)

	if err != nil {
		return err
	}

	// TODO Think About keeping stuff in sync
	if val, ok := dcm.cached.Load(filePath.String()); ok && !isDir {

		fname := val.(string)

		err = os.Remove(path.Join(dcm.Path, fname))

		if err != nil {
			return err
		}

		dcm.cached.Delete(filePath.String())
	}

	return nil
}

func (dcm *DiskCacheManager) Close(fd uint64) error {

	zap.L().Debug("Close",
		zap.Uint64("fd", fd),
	)

	if val, ok := dcm.opened.Load(fd); ok {

		fh := val.(*structure.CacheFileHandle)

		closeMsg := &structure.CloseMessage{
			Fd: fd,
		}

		payload := &structure.RequestPayload{
			Payload: &structure.RequestPayload_CloseMsg{
				CloseMsg: closeMsg,
			},
		}

		_, err := dcm.Sender.SendRequest(structure.CloseMessageCode, fh.FilePath.Address(), payload)

		if err != nil {
			return err
		}

		err = fh.Fp.Close()

		if err != nil {
			return err
		}

		dcm.opened.Delete(fd)

		return nil
	}

	return os.ErrInvalid
}

func (dcm *DiskCacheManager) Truncate(filePath *structure.RemotePath, size uint64) error {

	zap.L().Debug("Truncate",
		zap.String("remote-path", filePath.String()),
		zap.Uint64("size", size),
	)

	truncateMessage := &structure.TruncateMessage{
		Path: filePath.Path,
		Size: size,
	}

	payload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_TruncateMsg{
			TruncateMsg: truncateMessage,
		},
	}

	_, err := dcm.Sender.SendRequest(structure.TruncateMessageCode, filePath.Address(), payload)

	if err != nil {
		return err
	}

	if val, ok := dcm.cached.Load(filePath.String()); ok {
		err := os.Truncate(path.Join(dcm.Path, val.(string)), int64(size))

		if err != nil {
			return err
		}
	}

	return nil
}

func (dcm *DiskCacheManager) Flush(fd uint64) error {
	zap.L().Debug("Flush",
		zap.Uint64("fd", fd),
	)

	if val, ok := dcm.opened.Load(fd); ok {

		fh := val.(*structure.CacheFileHandle)

		flushMsg := &structure.FlushMessage{
			Fd: fd,
		}

		payload := &structure.RequestPayload{
			Payload: &structure.RequestPayload_FlushMsg{
				FlushMsg: flushMsg,
			},
		}

		_, err := dcm.Sender.SendRequest(structure.FlushMessageCode, fh.FilePath.Address(), payload)

		if err != nil {
			return err
		}

		err = fh.Fp.Sync()

		if err != nil {
			return err
		}

		return nil
	}

	return os.ErrInvalid
}

func (dcm *DiskCacheManager) Read(fd uint64, offset uint64, size uint64) ([]byte, error) {
	zap.L().Debug("Read",
		zap.Uint64("fd", fd),
		zap.Uint64("offset", offset),
		zap.Uint64("size", size),
	)

	if val, ok := dcm.opened.Load(fd); ok {
		fh := val.(*structure.CacheFileHandle)

		data := make([]byte, size)

		_, err := fh.Fp.ReadAt(data, int64(offset))

		// Get From Remote
		if err != nil && err != io.EOF {
			rm := &structure.ReadMessage{
				Fd:     fd,
				Offset: offset,
				Size:   size,
			}

			payload := &structure.RequestPayload{
				Payload: &structure.RequestPayload_ReadMsg{
					ReadMsg: rm,
				},
			}

			replyPayload, err := dcm.Sender.SendRequest(structure.ReadMessageCode, fh.FilePath.Address(), payload)

			if err != nil {
				return nil, err
			}

			return replyPayload.GetDataMsg().GetData(), nil
		}

		return data, nil
	}

	return nil, os.ErrInvalid
}

func (dcm *DiskCacheManager) Write(fd uint64, offset uint64, data []byte) (int, uint64, error) {
	zap.L().Debug("Write",
		zap.Uint64("fd", fd),
		zap.Uint64("offset", offset),
		zap.Int("size", len(data)),
	)

	if val, ok := dcm.opened.Load(fd); ok {
		fh := val.(*structure.CacheFileHandle)

		wm := &structure.WriteMessage{
			Fd:     fd,
			Offset: offset,
			Data:   data,
		}

		payload := &structure.RequestPayload{
			Payload: &structure.RequestPayload_WriteMsg{
				WriteMsg: wm,
			},
		}

		replyPayload, err := dcm.Sender.SendRequest(structure.WriteMessageCode, fh.FilePath.Address(), payload)

		if err != nil {
			return 0, 0, err
		}

		_, err = fh.Fp.WriteAt(data, int64(offset))

		if err != nil {
			return 0, 0, err
		}

		return int(replyPayload.GetWriteOkMsg().GetSize()), replyPayload.GetWriteOkMsg().GetFileSize(), err
	}

	return 0, 0, os.ErrInvalid
}

func (dcm *DiskCacheManager) Attr(filePath *structure.RemotePath) (*structure.FileInfo, error) {
	zap.L().Debug("Attr",
		zap.String("remote-path", filePath.String()),
	)

	am := &structure.AttrMessage{
		Path: filePath.Path,
	}

	payload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_AttrMsg{
			AttrMsg: am,
		},
	}

	replyPayload, err := dcm.Sender.SendRequest(structure.AttrMessageCode, filePath.Address(), payload)

	if err != nil {
		return nil, err
	}

	fi := &structure.FileInfo{
		Name:  replyPayload.GetFileInfoMsg().GetName(),
		Size:  replyPayload.GetFileInfoMsg().GetSize(),
		Mode:  replyPayload.GetFileInfoMsg().GetMode(),
		Mtime: replyPayload.GetFileInfoMsg().GetMtime(),
		Atime: replyPayload.GetFileInfoMsg().GetAtime(),
		IsDir: replyPayload.GetFileInfoMsg().GetIsDir(),
	}

	return fi, nil
}

func (dcm *DiskCacheManager) ReadDir(dirPath *structure.RemotePath) ([]*structure.FileInfo, error) {
	zap.L().Debug("ReadDir",
		zap.String("remote-path", dirPath.String()),
	)

	rdm := &structure.ReadDirMessage{
		Path: dirPath.Path,
	}

	payload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_ReadDirMsg{
			ReadDirMsg: rdm,
		},
	}

	replyPayload, err := dcm.Sender.SendRequest(structure.ReadDirMessageCode, dirPath.Address(), payload)

	if err != nil {
		return nil, err
	}

	var fileInfos []*structure.FileInfo

	for _, fim := range replyPayload.GetFileInfosMsg().GetFileInfos() {
		fi := &structure.FileInfo{
			Name:  fim.GetName(),
			Size:  fim.GetSize(),
			Mode:  fim.GetMode(),
			Mtime: fim.GetMtime(),
			Atime: fim.GetAtime(),
			IsDir: fim.GetIsDir(),
		}

		fileInfos = append(fileInfos, fi)
	}

	return fileInfos, nil
}

func (dcm *DiskCacheManager) SetMode(filePath *structure.RemotePath, mode uint32) error {
	zap.L().Debug("SetMode",
		zap.String("remote-filePath", filePath.String()),
		zap.String("mode", os.FileMode(mode).String()),
	)

	setModeMessage := &structure.SetModeMessage{
		Path: filePath.Path,
		Mode: mode,
	}

	payload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_SetModeMsg{
			SetModeMsg: setModeMessage,
		},
	}

	_, err := dcm.Sender.SendRequest(structure.SetModeMessageCode, filePath.Address(), payload)

	if err != nil {
		return err
	}

	return nil
}

func (dcm *DiskCacheManager) SetMtime(filePath *structure.RemotePath, mtime uint64, atime uint64) error {
	zap.L().Debug("SetMtime",
		zap.String("remote-filePath", filePath.String()),
		zap.String("mtime", time.Unix(0, int64(mtime)).String()),
		zap.String("atime", time.Unix(0, int64(atime)).String()),
	)

	setMtimeMessage := &structure.SetMtimeMessage{
		Path:  filePath.Path,
		Mtime: mtime,
		Atime: atime,
	}

	payload := &structure.RequestPayload{
		Payload: &structure.RequestPayload_SetMtimeMsg{
			SetMtimeMsg: setMtimeMessage,
		},
	}

	_, err := dcm.Sender.SendRequest(structure.SetMtimeMessageCode, filePath.Address(), payload)

	if err != nil {
		return err
	}

	return nil
}
