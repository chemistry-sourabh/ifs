package fs

import (
"log"
"os"

"bazil.org/fuse"
"bazil.org/fuse/fs"
"golang.org/x/net/context" // need this cause bazil lib doesn't use syslib context lib
)

type Dir struct {
	Node
	files       *[]*File
	directories *[]*Dir
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	log.Println("Requested Attr for Directory", d.name)
	a.Inode = d.inode
	a.Mode = os.ModeDir | 0444
	return nil
}
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.Println("Requested lookup for ", name)
	if d.files != nil {
		for _, n := range *d.files {
			if n.name == name {
				log.Println("Found match for directory lookup with size", len(n.data))
				return n, nil
			}
		}
	}
	if d.directories != nil {
		for _, n := range *d.directories {
			if n.name == name {
				log.Println("Found match for directory lookup")
				return n, nil
			}
		}
	}
	return nil, fuse.ENOENT
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	log.Println("Reading all dirs")
	var children []fuse.Dirent
	if d.files != nil {
		for _, f := range *d.files {
			children = append(children, fuse.Dirent{Inode: f.inode, Type: fuse.DT_File, Name: f.name})
		}
	}
	if d.directories != nil {
		for _, dir := range *d.directories {
			children = append(children, fuse.Dirent{Inode: dir.inode, Type: fuse.DT_Dir, Name: dir.name})
		}
		log.Println(len(children), " children for dir", d.name)
	}
	return children, nil
}

//func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
//	log.Println("Create request for name", req.Name)
//	f := &File{Node: Node{name: req.Name, inode: NewInode()}}
//	files := []*File{f}
//	if d.files != nil {
//		files = append(files, *d.files...)
//	}
//	d.files = &files
//	return f, f, nil
//}
//
//func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
//	log.Println("Remove request for ", req.Name)
//	if req.Dir && d.directories != nil {
//		newDirs := []*Dir{}
//		for _, dir := range *d.directories {
//			if dir.name != req.Name {
//				newDirs = append(newDirs, dir)
//			}
//		}
//		d.directories = &newDirs
//		return nil
//	} else if !req.Dir && *d.files != nil {
//		newFiles := []*File{}
//		for _, f := range *d.files {
//			if f.name != req.Name {
//				newFiles = append(newFiles, f)
//			}
//		}
//		d.files = &newFiles
//		return nil
//	}
//	return fuse.ENOENT
//}
//
//func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
//	log.Println("Mkdir request for name", req.Name)
//	dir := &Dir{Node: Node{name: req.Name, inode: NewInode()}}
//	directories := []*Dir{dir}
//	if d.directories != nil {
//		directories = append(*d.directories, directories...)
//	}
//	d.directories = &directories
//	return dir, nil
//
//}
