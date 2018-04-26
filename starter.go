package ifs

import (
	"bazil.org/fuse"
	log "github.com/sirupsen/logrus"
	"bazil.org/fuse/fs"
	"io/ioutil"
	"os"
	"path/filepath"
)

func generateVirtualNode(ifs *Ifs, paths string) *VirtualNode {

}



func generateVirtualNodes(ifs *Ifs, paths []string) map[string] *VirtualNode {

	aggPaths := make(map[string] []string)
	virtualNodes := make(map[string] *VirtualNode)

	for _, p := range paths {

		l := filepath.SplitList(p)

		firstDir := l[0]

		aggPaths[firstDir] = append(aggPaths[firstDir], p)

		//if _, ok := virtualNodes[firstDir]; !ok {
		//	virtualNodes[firstDir] = generateVirtualNode(ifs, p)
		//} else {
		//
		//}

	}

	for k,v := range aggPaths {

		virtualNodes[k] = &VirtualNode{
			Ifs:   ifs,
			Nodes: generateVirtualNodes(ifs, v),
		}
	}

	return virtualNodes
}

func generateRemoteRoot(ifs *Ifs, paths []string) *VirtualNode {

	return &VirtualNode{
		Ifs:   ifs,
		Nodes: generateVirtualNodes(ifs, paths),
	}
}

func generateRemoteRoots(ifs *Ifs, remoteRoots []*RemoteRoot) map[string]*VirtualNode {

	virtualNodes := make(map[string]*VirtualNode)

	for _, remoteRoot := range remoteRoots {
		vn := generateRemoteRoot(ifs, remoteRoot.StringArray())
		virtualNodes[remoteRoot.Hostname] = vn
	}

	return virtualNodes
}

func SetupLogger(cfg *LogConfig) {
	if !cfg.Logging {
		log.SetOutput(ioutil.Discard)
	} else if !cfg.Console {
		f, _ := os.Create(cfg.Path)
		//defer f.Close()
		log.SetOutput(f)
	}

	formatter := &log.TextFormatter{}
	formatter.DisableColors = true

	log.SetFormatter(formatter)

	if cfg.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

}

func MountRemoteRoots(cfg *FsConfig) {
	c, err := fuse.Mount(cfg.MountPoint)
	if err != nil {
		log.Fatal(err)
	}

	server := fs.New(c, nil)

	fileSystem := &Ifs{
		CachedStats: make(map[string]*Stat),
	}

	remoteRootNodes := generateRemoteNodes(fileSystem, cfg.RemoteRoots)

	talker := NewTalker(fileSystem)

	fileHandler := &FileHandler{
		Ifs: fileSystem,
	}

	hoarder := &Hoarder{
		Ifs:  fileSystem,
		Path: cfg.CacheLocation,
		Size: 100,
	}

	fileSystem.Talker = talker
	fileSystem.FileHandler = fileHandler
	fileSystem.Hoarder = hoarder
	fileSystem.RemoteRoots = remoteRootNodes

	talker.Startup(cfg.RemoteRoots[0].Address(), cfg.ConnCount)
	hoarder.Startup()
	fileHandler.StartUp()

	server.Serve(fileSystem)

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Panicln(err)
	}

	c.Close()
}
