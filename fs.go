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
	"fmt"
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
	log.WithFields(log.Fields{"root": true, "op": "attr"}).Debug("Got FS Op Request")
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
	log.WithFields(log.Fields{"root": true, "op": "readdir"}).Debug("Got FS Op Request")

	var children []fuse.Dirent

	for dirName := range root.RemoteRoots {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: dirName}
		children = append(children, child)
	}

	return children, nil
}

func (root *Ifs) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.WithFields(log.Fields{"root": true, "op": "lookup", "name": name}).Debug("Got FS Op Request")

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
	log.WithFields(log.Fields{
		"op": "attr",
		"address": rn.RemotePath.Address(),
		"path": rn.RemotePath.Path}).Debug("Got FS Op Request")

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
			"mod_time": time.Unix(0, s.ModTime)}).Debug("Got Op Response From Agent")
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
		log.WithFields(log.Fields{
			"op":      "attr",
			"address": rn.RemotePath.Address(),
			"path":    rn.RemotePath.Path,
		}).Error("Got Op Response as error", err)
	}

	return err
}

func (rn *RemoteNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	// Get Files from Remote Directory
	// Populate Directory Accordingly
	log.WithFields(log.Fields{
		"op":"readdir",
		"address":rn.RemotePath.Address(),
		"path": rn.RemotePath.Path}).Debug("Got FS Op Request")

	resp := rn.Ifs.Talker.sendRequest(ReadDirRequest, rn.RemotePath)

	var children []fuse.Dirent
	rn.RemoteNodes = make(map[string]*RemoteNode)

	var err error
	if respError, ok := resp.Data.(Error); !ok {

		// TODO Cache these for future Attr Requests!!
		files := resp.Data.(*DirInfo).Stats

		for _, file := range files {

			s := file

			rn.Ifs.CachedStats[AppendFileToRemotePath(rn.RemotePath, s.Name)] = s

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
	log.Printf("Lookup %s", name)

	val, ok := rn.RemoteNodes[name]

	if ok {
		return val, nil
	} else {
		return nil, fuse.ENOENT
	}
}

// TODO Open for Dir also
func (rn *RemoteNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	log.Println("Open call on file", rn.RemotePath.String())

	var err error
	if !rn.IsDir {
		err = rn.Ifs.FileHandler.OpenFile(rn.RemotePath)

	}
	return rn, err

}

func (rn *RemoteNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.Printf("Read %s off=%d size=%d", rn.RemotePath.String(), req.Offset, req.Size)

	b, err := rn.Ifs.FileHandler.ReadData(rn.RemotePath, req.Offset, req.Size)

	resp.Data = b

	return err
}

func (rn *RemoteNode) ReadAll(ctx context.Context) ([]byte, error) {
	log.Println("Reading all of file", rn.RemotePath.Path)

	data, err := rn.Ifs.FileHandler.ReadAllData(rn.RemotePath)

	return data, err

}

// TODO Think About Append Mode
func (rn *RemoteNode) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	log.Println("Trying to write to ", rn.RemotePath.String(), "offset", req.Offset, "dataSize:", len(req.Data))
	fmt.Println(req.Data)

	n, err := rn.Ifs.FileHandler.WriteData(rn.RemotePath, req.Data, req.Offset)
	resp.Size = n
	log.Println("Wrote to file ", rn.RemotePath.String())
	return err
}

func (rn *RemoteNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	log.Printf("Setattr %s %t %d", rn.RemotePath.String(), req.Valid.Size(), req.Size)

	var err error
	if req.Valid.Size() {
		err = rn.Ifs.FileHandler.Truncate(rn.RemotePath, req.Size)
	}

	return err
}

func (rn *RemoteNode) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	log.Println("Flushing file ", rn.RemotePath.String())
	return nil
}

func (rn *RemoteNode) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	log.Println("Release requested on file", rn.RemotePath.String())
	rn.Ifs.FileHandler.Release(rn.RemotePath)
	return nil
}

func (rn *RemoteNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	log.Println("Fsync call on file", rn.RemotePath.String())
	return nil
}

func (rn *RemoteNode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	log.Println("Creating file ", rn.RemotePath.String(), req.Name)

	// Create File Remotely
	// Create File in Cache if Space is available
	// File should be in open state
	// Return Errors
	err := rn.Ifs.FileHandler.Create(rn.RemotePath, req.Name)
	if err == nil {
		newRn := rn.generateChildRemoteNode(req.Name, false)
		rn.RemoteNodes[req.Name] = newRn
		return newRn, newRn, nil
	}

	return nil, nil, err
}

func (rn *RemoteNode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	log.Println("Making Dir ", rn.RemotePath.String(), req.Name)

	err := rn.Ifs.FileHandler.Mkdir(rn.RemotePath, req.Name)

	if err == nil {
		newRn := rn.generateChildRemoteNode(req.Name, true)
		rn.RemoteNodes[req.Name] = newRn
		return newRn, nil
	}

	return nil, err
}

func (rn *RemoteNode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	log.Println("Removing Node", rn.RemotePath.String(), req.Name)

	err := rn.Ifs.FileHandler.Remove(rn.RemotePath, req.Name)
	if err == nil {
		delete(rn.RemoteNodes, req.Name)
	}
	return err
}
