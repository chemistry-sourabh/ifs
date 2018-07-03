package ifs

import (
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"path"
	"os"
	log "github.com/sirupsen/logrus"
)

type Watcher struct {
	Agent   *Agent
	Paths   map[string]bool
	watcher *fsnotify.Watcher
}

func (w *Watcher) Startup() error {

	w.Paths = make(map[string]bool)

	watcher, err := fsnotify.NewWatcher()

	if err != nil {
		return err
	}

	w.watcher = watcher

	go w.processEvents()

	return nil
}

func (w *Watcher) processEvents() {

	for {
		select {
		case event := <-w.watcher.Events:
			w.processEvent(event)
			//case err := <-w.watcher.Errors:
		}
	}

}

func (w *Watcher) processEvent(event fsnotify.Event) {

	// If folder is created should be added to watch list
	// Will need to send attr back
	if event.Op&fsnotify.Create == fsnotify.Create {
	} else if event.Op&fsnotify.Write == fsnotify.Write {
		// If Write happened then will need to update cache with write
	} else if event.Op&fsnotify.Remove == fsnotify.Remove {
		// If Deleted then will need to be sent back
	} else if event.Op&fsnotify.Rename == fsnotify.Rename {

	} else if event.Op&fsnotify.Chmod == fsnotify.Chmod {
		// Simple Attr Update

		payload := &AttrUpdateInfo{}

		info, err := os.Stat(event.Name)

		// TODO Log Error
		if err == nil {

			log.WithFields(log.Fields{
				"op":    "chmod",
				"path":  event.Name,
				"size":  info.Size(),
				"mode":  info.Mode(),
				"mtime": info.ModTime(),
			}).Debug("Got Watch Event")

			payload.Path = event.Name
			payload.Size = info.Size()
			payload.Mode = info.Mode()
			payload.ModTime = info.ModTime().UnixNano()

			pkt := &Packet{
				Op:    AttrUpdateRequest,
				Flags: 0,
				Data:  payload,
			}

			w.Agent.Talker.SendPacket(pkt)

		}
	}

}

func (w *Watcher) watchDir(dirPath string) error {
	//w.watcher.Add(dirPath)

	var allDirs []string
	dirs := []string{dirPath}

	for len(dirs) > 0 {
		filePath := dirs[0]

		allDirs = append(allDirs, filePath)

		dirs = dirs[1:]

		files, err := ioutil.ReadDir(filePath)

		if err != nil {
			return err
		}

		for _, f := range files {
			if f.IsDir() {
				fullPath := path.Join(filePath, f.Name())
				dirs = append(dirs, fullPath)
			}
		}

	}

	for _, dir := range allDirs {
		w.Paths[dir] = true
		w.watcher.Add(dir)
	}

	return nil
}

func (w *Watcher) WatchPaths(request *Packet) error {

	watchInfo := request.Data.(*WatchInfo)

	var err error

	for _, p := range watchInfo.Paths {
		err = w.watchDir(p)

		if err != nil {
			return err
		}
	}

	return nil
}
