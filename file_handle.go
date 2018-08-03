package ifs

import "bazil.org/fuse"
import (
	"golang.org/x/net/context"
	"time"
	"go.uber.org/zap"
	"github.com/orcaman/concurrent-map"
	)

type FileHandle struct {
	RemoteNode     *RemoteNode
	FileDescriptor uint64
}

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	rn := fh.RemoteNode

	zap.L().Debug("Read FS Request",
		zap.String("op", "read"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.Int64("offset", req.Offset),
		zap.Int("size", req.Size),
		zap.Uint64("fd", fh.FileDescriptor),
	)

	b, err := FileHandler().ReadData(fh, req.Offset, req.Size)

	resp.Data = b

	if err != nil {

		zap.L().Warn("Read Error Response",
			zap.String("op", "read"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Int64("offset", req.Offset),
			zap.Int("size", req.Size),
			zap.Uint64("fd", fh.FileDescriptor),
			zap.Error(err),
		)

	}

	return err
}

func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	rn := fh.RemoteNode

	zap.L().Debug("Write FS Request",
		zap.String("op", "write"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
		zap.Int64("offset", req.Offset),
		zap.Int("size", len(req.Data)),
	)

	n, err := FileHandler().WriteData(fh, req.Data, req.Offset)
	resp.Size = n

	if err != nil {

		zap.L().Warn("Write Error Response",
			zap.String("op", "write"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Int64("offset", req.Offset),
			zap.Int("size", len(req.Data)),
			zap.Error(err),
		)

	}

	return err
}

// TODO Remove Nodes if not present on remote
func (fh *FileHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {

	rn := fh.RemoteNode

	zap.L().Debug("ReadDir FS Request",
		zap.String("op", "readdir"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
	)

	// Get Files from Remote Directory
	// Populate Directory Accordingly

	req := &ReadDirInfo{
		Path:           rn.RemotePath.Path,
		FileDescriptor: fh.FileDescriptor,
	}

	resp := Talker().sendRequest(ReadDirRequest, rn.RemotePath.Hostname, req)

	var children []fuse.Dirent
	//rn.RemoteNodes = make(map[string] *RemoteNode)

	var err error
	if respError, ok := resp.Data.(Error); !ok {

		files := resp.Data.(*DirInfo).Stats

		zap.L().Debug("ReadDir Response From Agent",
			zap.String("op", "readdir"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Int("size", len(files)),
		)

		newRns := cmap.New()

		for _, file := range files {

			s := file

			zap.L().Debug("ReadDir File Response",
				zap.String("op", "readdir"),
				zap.String("address", rn.RemotePath.Address()),
				zap.String("path", rn.RemotePath.Path),
				zap.Int64("size", s.Size),
				zap.String("mode", s.Mode.String()),
				zap.Time("mtime", time.Unix(0, s.ModTime)),
			)

			var child fuse.Dirent
			if s.IsDir {
				child = fuse.Dirent{Type: fuse.DT_Dir, Name: s.Name}
			} else {
				child = fuse.Dirent{Type: fuse.DT_File, Name: s.Name}
			}
			children = append(children, child)

			val, ok := rn.RemoteNodes.Get(s.Name)

			var newRn *RemoteNode

			if !ok {
				newRn = rn.generateChildRemoteNode(s.Name, s.IsDir)
			} else {
				newRn = val.(*RemoteNode)
			}

			newRn.Size = uint64(s.Size)
			newRn.Mode = s.Mode
			newRn.Mtime = time.Unix(0, s.ModTime)
			newRn.IsCached = true

			newRns.Set(s.Name, newRn)
			//rn.RemoteNodes[s.Name] = newRn

		}

		//TODO Might be fishy (Atomic?)
		rn.RemoteNodes = &newRns

		return children, nil

	} else {
		err = respError.Err

		zap.L().Warn("ReadDir Error Response",
			zap.String("op", "readdir"),
			zap.String("address", rn.RemotePath.Address()),
			zap.String("path", rn.RemotePath.Path),
			zap.Error(err),
		)

	}
	return nil, err
}

func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {

	rn := fh.RemoteNode

	zap.L().Debug("Flush FS Request",
		zap.String("op", "flush"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
	)

	//rn.Ifs.fileHandler.Flush(fh)

	return nil
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {

	rn := fh.RemoteNode

	zap.L().Debug("Release FS Request",
		zap.String("op", "release"),
		zap.String("address", rn.RemotePath.Address()),
		zap.String("path", rn.RemotePath.Path),
	)

	FileHandler().Release(fh)
	return nil
}
