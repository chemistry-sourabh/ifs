package ifs

import "bazil.org/fuse"
import log "github.com/sirupsen/logrus"
import (
	"golang.org/x/net/context"
	"time"
	"path"
)

type FileHandle struct {
	RemoteNode     *RemoteNode
	FileDescriptor uint64
}

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	rn := fh.RemoteNode

	fields := log.Fields{
		"op":      "read",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
		"offset":  req.Offset,
		"size":    req.Size,
		"fd":      fh.FileDescriptor,
	}
	log.WithFields(fields).Debug("Read FS Request")

	b, err := rn.Ifs.FileHandler.ReadData(fh, req.Offset, req.Size)

	resp.Data = b

	if err != nil {
		log.WithFields(fields).Warn("Read Error Response:", err)
	}

	return err
}

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	rn := fh.RemoteNode

	fields := log.Fields{
		"op":      "write",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
		"offset":  req.Offset,
		"size":    len(req.Data),
	}
	log.WithFields(fields).Debug("Write FS Request")

	n, err := rn.Ifs.FileHandler.WriteData(fh, req.Data, req.Offset)
	resp.Size = n

	if err != nil {
		log.WithFields(fields).Warn("Write Error Response:", err)
	}

	return err
}

func (fh *FileHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {

	rn := fh.RemoteNode

	// Get Files from Remote Directory
	// Populate Directory Accordingly
	fields := log.Fields{
		"op":      "readdir",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
	}
	log.WithFields(fields).Debug("ReadDir FS Request")

	req := &ReadDirInfo{
		Path:     rn.RemotePath.Path,
		FileDescriptor: fh.FileDescriptor,
	}

	resp := rn.Ifs.Talker.sendRequest(ReadDirRequest, rn.RemotePath.Hostname, req)

	var children []fuse.Dirent
	//rn.RemoteNodes = make(map[string] *RemoteNode)

	var err error
	if respError, ok := resp.Data.(Error); !ok {

		// TODO Cache these for future Attr Requests!!
		files := resp.Data.(*DirInfo).Stats

		log.WithFields(log.Fields{
			"op":      "readdir",
			"address": rn.RemotePath.Address(),
			"path":    rn.RemotePath.Path,
			"size":    len(files),
		}).Debug("ReadDir Response from Agent")

		for _, file := range files {

			s := file

			log.WithFields(log.Fields{
				"op":      "readdir",
				"address": rn.RemotePath.Address(),
				"path":    path.Join(rn.RemotePath.Path, s.Name),
				"size":    s.Size,
				"mode":    s.Mode,
				"mtime":   s.ModTime,
			}).Debug("ReadDir File Response")

			var child fuse.Dirent
			if s.IsDir {
				child = fuse.Dirent{Type: fuse.DT_Dir, Name: s.Name}
			} else {
				child = fuse.Dirent{Type: fuse.DT_File, Name: s.Name}
			}
			children = append(children, child)

			newRn, ok := rn.RemoteNodes[s.Name]

			if !ok {
				newRn = rn.generateChildRemoteNode(s.Name, s.IsDir)
				rn.RemoteNodes[s.Name] = newRn
			}

			newRn.Size = uint64(s.Size)
			newRn.Mode = s.Mode
			newRn.Mtime = time.Unix(0, s.ModTime)
			newRn.IsCached = true

			//rn.RemoteNodes[s.Name] = newRn

		}

		return children, nil

	} else {
		err = respError.Err
		log.WithFields(fields).Warn("ReadDir Error Response:", err)
	}
	return nil, err
}

func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {

	rn := fh.RemoteNode

	log.WithFields(log.Fields{
		"op":      "flush",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
	}).Debug("Flush FS Request")

	//rn.Ifs.FileHandler.Flush(fh)

	return nil
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	rn := fh.RemoteNode

	log.WithFields(log.Fields{
		"op":      "release",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
	}).Debug("Release FS Request")

	rn.Ifs.FileHandler.Release(fh)
	return nil
}
