package ifs

import (
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"path"
	"fmt"
)

type Watcher struct {
	Agent   *Agent
	Paths   []string
	watcher *fsnotify.Watcher
}

func (w *Watcher) Startup() error {
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

	fmt.Println(event.String())
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

		//payload := &AttrUpdateInfo{}
		//
		//info, err := os.Stat(event.Name)
		//
		//payload.RemotePath = event.Name
		//payload.Size = info.Size()
		//payload.Mode = info.Mode()
		//payload.ModTime = info.ModTime().UnixNano()

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
		w.Paths = append(w.Paths, dir)
		w.watcher.Add(dir)
	}

	return nil
}
