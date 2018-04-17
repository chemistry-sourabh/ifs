package ifs

import (
	"bazil.org/fuse"
	"golang.org/x/net/context"
	"bazil.org/fuse/fs"
	log "github.com/sirupsen/logrus"
	"path"
	"os/user"
	"strconv"
	"os"
	"time"
)

type Ifs struct {
	Talker      *Talker
	FileHandler *FileHandler
	Hoarder     *Hoarder
	RemoteRoots map[string]*RemoteNode
	CachedStats map[string]*Stat
}

// TODO All Errors should be resolved here
func (root *Ifs) Root() (fs.Node, error) {
	return root, nil
}

func (root *Ifs) Attr(ctx context.Context, attr *fuse.Attr) error {
	log.WithFields(log.Fields{"root": true, "op": "attr"}).Debug("Attr FS Request")
	// Check Error
	curUser, _ := user.Current()
	uid, _ := strconv.ParseUint(curUser.Uid, 10, 64)

	curGroup, _ := user.LookupGroup("staff")
	gid, _ := strconv.ParseUint(curGroup.Gid, 10, 64)

	attr.Uid = uint32(uid)
	attr.Gid = uint32(gid)
	//attr.Size = uint64(10)
	attr.Mode = os.FileMode(os.ModeDir | 0666)
	//attr.Mtime = s.ModTime

	return nil
}

func (root *Ifs) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.WithFields(log.Fields{"root": true, "op": "readdir"}).Debug("ReadDir FS Request")

	var children []fuse.Dirent

	for dirName := range root.RemoteRoots {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: dirName}
		children = append(children, child)
	}

	return children, nil
}

func (root *Ifs) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.WithFields(log.Fields{"root": true, "op": "lookup", "name": name}).Debug("Lookup FS Request")

	val, ok := root.RemoteRoots[name]

	if ok {
		return val, nil
	} else {
		return nil, fuse.ENOENT
	}
}

type RemoteNode struct {
	Ifs         *Ifs                   `msgpack:"-"`
	IsDir       bool
	RemotePath  *RemotePath
	RemoteNodes map[string]*RemoteNode `msgpack:"-"`
}

func (rn *RemoteNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	// Update FileHandler if invalid
	fields := log.Fields{
		"op":      "attr",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
	}

	log.WithFields(fields).Debug("Attr FS Request")

	var resp *Packet
	//if stat, ok := rn.Ifs.CachedStats[rn.RemotePath.String()]; ok {
	//	resp = &Packet{
	//		Data: stat,
	//	}
	//	delete(rn.Ifs.CachedStats, rn.RemotePath.String())
	//} else {
	resp = rn.Ifs.Talker.sendRequest(AttrRequest, rn.RemotePath)
	//log.Printf("Got Response for Attr %s", rn.RemotePath.String())
	//}

	var err error = nil
	if respErr, ok := resp.Data.(Error); !ok {

		s := resp.Data.(*Stat)
		log.WithFields(log.Fields{
			"op":       "attr",
			"address":  rn.RemotePath.Address(),
			"path":     rn.RemotePath.Path,
			"mode":     s.Mode,
			"size":     s.Size,
			"mod_time": time.Unix(0, s.ModTime)}).Debug("Attr Response From Agent")
		// Check Error
		curUser, _ := user.Current()
		uid, _ := strconv.ParseUint(curUser.Uid, 10, 64)

		curGroup, _ := user.LookupGroup("staff")
		gid, _ := strconv.ParseUint(curGroup.Gid, 10, 64)

		attr.Uid = uint32(uid)
		attr.Gid = uint32(gid)
		attr.Size = uint64(s.Size)
		attr.Mode = s.Mode
		attr.Mtime = time.Unix(0, s.ModTime)

	} else {
		err = respErr.Err
		log.WithFields(fields).Error("Attr Error Response:", err)
	}

	return err
}

func (rn *RemoteNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	// Get Files from Remote Directory
	// Populate Directory Accordingly
	fields := log.Fields{
		"op":      "readdir",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
	}
	log.WithFields(fields).Debug("ReadDir FS Request")

	resp := rn.Ifs.Talker.sendRequest(ReadDirRequest, rn.RemotePath)

	var children []fuse.Dirent
	rn.RemoteNodes = make(map[string]*RemoteNode)

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

			//rn.Ifs.CachedStats[AppendFileToRemotePath(rn.RemotePath, s.Name)] = s

			var child fuse.Dirent
			if s.IsDir {
				child = fuse.Dirent{Type: fuse.DT_Dir, Name: s.Name}
			} else {
				child = fuse.Dirent{Type: fuse.DT_File, Name: s.Name}
			}
			children = append(children, child)
			rn.RemoteNodes[s.Name] = rn.generateChildRemoteNode(s.Name, s.IsDir)

		}

		return children, nil

	} else {
		err = respError.Err
		log.WithFields(fields).Error("ReadDir Error Response:", err)
	}
	return nil, err
}

func (rn *RemoteNode) generateChildRemoteNode(name string, isDir bool) *RemoteNode {
	return &RemoteNode{
		Ifs:   rn.Ifs,
		IsDir: isDir,
		RemotePath: &RemotePath{
			Hostname: rn.RemotePath.Hostname,
			Port:     rn.RemotePath.Port,
			Path:     path.Join(rn.RemotePath.Path, name),
		},
		RemoteNodes: make(map[string]*RemoteNode),
	}
}

func (rn *RemoteNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.WithFields(log.Fields{
		"op":      "lookup",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
		"name":    name,
	}).Debug("Lookup FS Request")

	val, ok := rn.RemoteNodes[name]

	log.WithFields(log.Fields{
		"op":      "lookup",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
		"name":    name,
		"ok":      ok,
	}).Debug("Lookup Response")

	if ok {
		return val, nil
	} else {
		return nil, fuse.ENOENT
	}
}

// TODO Open for Dir also
func (rn *RemoteNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	fields := log.Fields{
		"op":      "open",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
	}
	log.WithFields(fields).Debug("Open FS Request")

	var err error
	if !rn.IsDir {
		err = rn.Ifs.FileHandler.OpenFile(rn.RemotePath)
	}

	if err != nil {
		log.WithFields(fields).Error("Open Error Response:", err)
	}

	return rn, err

}

func (rn *RemoteNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fields := log.Fields{
		"op":      "read",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
		"offset":  req.Offset,
		"size":    req.Size,
	}
	log.WithFields(fields).Debug("Read FS Request")

	b, err := rn.Ifs.FileHandler.ReadData(rn.RemotePath, req.Offset, req.Size)

	resp.Data = b

	if err != nil {
		log.WithFields(fields).Error("Read Error Response:", err)
	}

	return err
}

func (rn *RemoteNode) ReadAll(ctx context.Context) ([]byte, error) {
	fields := log.Fields{
		"op":      "readall",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
	}
	log.WithFields(fields).Debug("ReadAll FS Request")

	data, err := rn.Ifs.FileHandler.ReadAllData(rn.RemotePath)

	if err != nil {
		log.WithFields(fields).Error("ReadAll Error Response:", err)
	}

	return data, err

}

// TODO Think About Append Mode
func (rn *RemoteNode) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fields := log.Fields{
		"op":      "write",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
		"offset":  req.Offset,
		"size":    len(req.Data),
	}
	log.WithFields(fields).Debug("Write FS Request")

	n, err := rn.Ifs.FileHandler.WriteData(rn.RemotePath, req.Data, req.Offset)
	resp.Size = n

	if err != nil {
		log.WithFields(fields).Error("Write Error Response:", err)
	}

	return err
}

func (rn *RemoteNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// TODO Add other attributes
	fields := log.Fields{
		"op":      "setattr",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
		"valid":   req.Valid.String(),
		"size":    req.Size,
	}
	log.WithFields(fields).Debug("SetAttr FS Request")

	var err error
	if req.Valid.Size() {
		err = rn.Ifs.FileHandler.Truncate(rn.RemotePath, req.Size)
	}

	if err != nil {
		log.WithFields(fields).Debug("SetAttr Error Response", err)
	}

	return err
}

func (rn *RemoteNode) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	log.WithFields(log.Fields{
		"op":      "flush",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
	}).Debug("Flush FS Request")
	return nil
}

func (rn *RemoteNode) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	log.WithFields(log.Fields{
		"op":      "release",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
	}).Debug("Release FS Request")
	rn.Ifs.FileHandler.Release(rn.RemotePath)
	return nil
}

func (rn *RemoteNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	log.WithFields(log.Fields{
		"op":      "fsync",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
	}).Debug("Fsync FS Request")
	return nil
}

func (rn *RemoteNode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	fields := log.Fields{
		"op":      "create",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
		"name":    req.Name,
	}

	log.WithFields(fields).Debug("Create FS Request")

	// Create File Remotely
	// Create File in Cache if Space is available
	// File should be in open state
	// Return Errors
	err := rn.Ifs.FileHandler.Create(rn.RemotePath, req.Name)
	if err == nil {
		newRn := rn.generateChildRemoteNode(req.Name, false)
		rn.RemoteNodes[req.Name] = newRn
		return newRn, newRn, nil
	} else {
		log.WithFields(fields).Error("Create Error Response:", err)
	}

	return nil, nil, err
}

func (rn *RemoteNode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	fields := log.Fields{
		"op":      "mkdir",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
		"name":    req.Name,
	}

	log.WithFields(fields).Debug("Mkdir FS request")

	err := rn.Ifs.FileHandler.Mkdir(rn.RemotePath, req.Name)

	if err == nil {
		newRn := rn.generateChildRemoteNode(req.Name, true)
		rn.RemoteNodes[req.Name] = newRn
		return newRn, nil
	} else {
		log.WithFields(fields).Error("Mkdir Error Response:", err)
	}

	return nil, err
}

func (rn *RemoteNode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	fields := log.Fields{
		"op":      "remove",
		"address": rn.RemotePath.Address(),
		"path":    rn.RemotePath.Path,
		"name":    req.Name,
	}
	log.WithFields(fields).Debug("Remove FS Request")

	err := rn.Ifs.FileHandler.Remove(rn.RemotePath, req.Name)
	if err == nil {
		delete(rn.RemoteNodes, req.Name)
	} else {
		log.WithFields(fields).Error("Remove Error Response", err)
	}
	return err
}
