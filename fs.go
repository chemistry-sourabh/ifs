package arsyncfs

import (
	"bazil.org/fuse"
	"golang.org/x/net/context"
	"bazil.org/fuse/fs"
	"log"
	"path"
	"os/user"
	"strconv"
	"os"
	"time"
)

type Ifs struct {
	Talker      *Talker
	FileHandler *FileHandler
	RemoteRoots map[string]*RemoteNode
}

func (root *Ifs) Root() (fs.Node, error) {
	return root, nil
}

func (root *Ifs) Attr(ctx context.Context, attr *fuse.Attr) error {
	log.Println("Root Attr")
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
	log.Printf("Root ReadDirAll")

	var children []fuse.Dirent

	for dirName := range root.RemoteRoots {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: dirName}
		children = append(children, child)
	}

	return children, nil
}

func (root *Ifs) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.Printf("Root Lookup %s", name)

	val, ok := root.RemoteRoots[name]

	if ok {
		return val, nil
	} else {
		return nil, fuse.ENOENT
	}
}

type RemoteNode struct {
	Ifs         *Ifs                   `json:"-"`
	IsDir       bool                   `json:"is-dir"`
	RemotePath  *RemotePath            `json:"remote-path"`
	RemoteNodes map[string]*RemoteNode `json:"-"`
}

func (rn *RemoteNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	// Update FileHandler if invalid

	log.Printf("Attr %s \n", rn.RemotePath.String())

	resp := rn.Ifs.Talker.sendRequest(AttrOp, rn)
	log.Printf("Got Response for Attr %s", rn.RemotePath.String())
	s := resp.(*StatResponse).Stat

	log.Println(s)

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

	return nil
}

func (rn *RemoteNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	// Get Files from Remote Directory
	// Populate Directory Accordingly
	log.Printf("ReadDir %s", rn.RemotePath.String())

	resp := rn.Ifs.Talker.sendRequest(ReadDirOp, rn)

	var children []fuse.Dirent
	rn.RemoteNodes = make(map[string]*RemoteNode)

	files := resp.(*ReadDirResponse).Stats

	for _, file := range files {

		s := file

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

func (rn *RemoteNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	log.Println("Open call on file", rn.RemotePath.String())
	if !rn.IsDir {
		rn.Ifs.FileHandler.OpenFile(rn)
	}
	return rn, nil

}

func (rn *RemoteNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.Printf("Read %s off=%d size=%d", rn.RemotePath.String(), req.Offset, req.Size)

	b, _, err := rn.Ifs.FileHandler.ReadData(rn, req.Offset, req.Size)

	resp.Data = b

	return err
}

//
//func (rn *RNode) ReadAll(ctx context.Context) ([]byte, error) {
//	log.Println("Reading all of file", rn.RemotePath.Path)
//
//	creq := &CacheRequest{
//		Op: GetLocalFileCacheOp,
//		RNode: rn,
//		ResponseChannel: make(chan []byte),
//	}
//
//	rn.CacheRequestChannel <- creq
//
//	return <- creq.ResponseChannel, nil
//}

//func (rn *RNode) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
//	log.Println("Trying to write to ", rn.RemoteRoot.String(), "offset", req.Offset, "dataSize:", len(req.Data), "Data: ", string(req.Data))
//
//
//	//resp.Size = len(req.Data)
//	//f.Data = req.Data
//	log.Println("Wrote to file", f.name)
//	return nil
//}
