package ifs

import (
	"bazil.org/fuse"
	"golang.org/x/net/context"
	"bazil.org/fuse/fs"
	log "github.com/sirupsen/logrus"
	"os/user"
	"strconv"
	"os"
)

type Ifs struct {
	Talker      *Talker
	FileHandler *FileHandler
	Hoarder     *Hoarder
	RemoteRoots map[string]*fs.Node
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

