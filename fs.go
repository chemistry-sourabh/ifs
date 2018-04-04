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
	"bazil.org/fuse/fuseutil"
	"io"
	"bufio"
)

type Root struct {
	RemoteRoots map[string]*RemoteNode
}

func (root *Root) Root() (fs.Node, error) {
	return root, nil
}

func (root *Root) Attr(ctx context.Context, attr *fuse.Attr) error {
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

func (root *Root) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Printf("Root ReadDirAll")

	var children []fuse.Dirent

	for _, rootDir := range root.RemoteRoots {
		child := fuse.Dirent{Type: fuse.DT_Dir, Name: rootDir.RemotePath.Address()}
		children = append(children, child)
	}

	return children, nil
}

func (root *Root) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.Printf("Root Lookup %s", name)

	val, ok := root.RemoteRoots[name]

	if ok {
		return val, nil
	} else {
		return nil, fuse.ENOENT
	}
}

type RemoteNode struct {
	IsDir                bool                   `json:"is-dir"`
	IsCached             bool                   `json:"is-cached"`
	RemotePath           *RemotePath            `json:"remote-path"`
	EgressRequestChannel chan *Request          `json:"-"`
	CacheRequestChannel  chan *CacheRequest     `json:"-"`
	RemoteNodes          map[string]*RemoteNode `json:"-"`
}

func (rn *RemoteNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	// Update Cache if invalid

	respChannel := make(chan *Response)

	log.Printf("Attr %s \n", rn.RemotePath.String())

	req := &Request{
		Op:              AttrOp,
		RemoteNode:      rn,
		ResponseChannel: respChannel,
	}

	rn.EgressRequestChannel <- req
	resp := <-respChannel

	s := ConvertToStat(resp.Response)

	// Check Error
	curUser, _ := user.Current()
	uid, _ := strconv.ParseUint(curUser.Uid, 10, 64)

	curGroup, _ := user.LookupGroup("staff")
	gid, _ := strconv.ParseUint(curGroup.Gid, 10, 64)

	attr.Uid = uint32(uid)
	attr.Gid = uint32(gid)
	attr.Size = uint64(s.Size)
	attr.Mode = s.Mode
	attr.Mtime = s.ModTime

	return nil
}

func (rn *RemoteNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	// Get Files from Remote Directory
	// Populate Directory Accordingly
	log.Printf("ReadDir %s", rn.RemotePath.String())

	respChannel := make(chan *Response)

	req := &Request{
		Op:              ReadDirOp,
		RemoteNode:      rn,
		ResponseChannel: respChannel,
	}

	rn.EgressRequestChannel <- req

	resp := <-respChannel

	var children []fuse.Dirent
	rn.RemoteNodes = make(map[string]*RemoteNode)

	files := resp.Response.([]interface{})

	for _, file := range files {

		s := ConvertToStat(file)

		if s.IsDir {
			child := fuse.Dirent{Type: fuse.DT_Dir, Name: s.Name}
			children = append(children, child)

			newRn := &RemoteNode{
				IsDir: true,
				RemotePath: &RemotePath{
					Hostname: rn.RemotePath.Hostname,
					Port:     rn.RemotePath.Port,
					Path:     path.Join(rn.RemotePath.Path, s.Name),
				},
				EgressRequestChannel: rn.EgressRequestChannel,
				CacheRequestChannel: rn.CacheRequestChannel,
			}

			rn.RemoteNodes[s.Name] = newRn

		} else {
			child := fuse.Dirent{Type: fuse.DT_File, Name: s.Name}
			children = append(children, child)

			newRn := &RemoteNode{
				IsDir: false,
				RemotePath: &RemotePath{
					Hostname: rn.RemotePath.Hostname,
					Port:     rn.RemotePath.Port,
					Path:     path.Join(rn.RemotePath.Path, s.Name),
				},
				EgressRequestChannel: rn.EgressRequestChannel,
				CacheRequestChannel: rn.CacheRequestChannel,
			}

			rn.RemoteNodes[s.Name] = newRn
		}

	}

	return children, nil
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

	// If not cached then get file from remote
	// Else open file from Cache

	if !rn.IsCached && !rn.IsDir {
		log.Println("Generate Cache Request")
		creq := &CacheRequest{
			Op: FetchFileCacheOp,
			RemoteNode: rn,
		}

		rn.CacheRequestChannel <- creq
	}

	return rn, nil

}

func (rn *RemoteNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.Println("Requested Read on File", rn.RemotePath.String())

	creq := &CacheRequest{
		Op: GetLocalFileCacheOp,
		RemoteNode: rn,
		ResponseChannel: make(chan []byte),
	}


	rn.CacheRequestChannel <- creq

	fuseutil.HandleRead(req, resp, <- creq.ResponseChannel)
	return nil
}

func (rn *RemoteNode) ReadAll(ctx context.Context) ([]byte, error) {
	log.Println("Reading all of file", rn.RemotePath.Path)

	creq := &CacheRequest{
		Op: GetLocalFileCacheOp,
		RemoteNode: rn,
		ResponseChannel: make(chan []byte),
	}

	rn.CacheRequestChannel <- creq

	return <- creq.ResponseChannel, nil
}

//func (rn *RemoteNode) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
//	log.Println("Trying to write to ", rn.RemotePath.String(), "offset", req.Offset, "dataSize:", len(req.Data), "data: ", string(req.Data))
//
//
//	//resp.Size = len(req.Data)
//	//f.data = req.Data
//	log.Println("Wrote to file", f.name)
//	return nil
//}
