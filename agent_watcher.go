package ifs

import (
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"path"
	"os"
	"go.uber.org/zap"
	"sync"
)

type watcher struct {
	//Agent   *Agent
	Paths   map[string]bool
	watcher *fsnotify.Watcher
}

var (
	watcherInstance *watcher
	watcherOnce     sync.Once
)

func Watcher() *watcher {
	watcherOnce.Do(func() {
		watcherInstance = &watcher{
			Paths: make(map[string]bool),
		}
	})

	return watcherInstance
}

func (w *watcher) Startup() error {

	watcher, err := fsnotify.NewWatcher()

	if err != nil {
		return err
	}

	w.watcher = watcher

	go w.processEvents()

	return nil
}

func (w *watcher) processEvents() {

	for {
		select {
		case event := <-w.watcher.Events:
			w.processEvent(event)
			//case err := <-w.watcher.Errors:
		}
	}

}

//TODO Finish Watcher
func (w *watcher) processEvent(event fsnotify.Event) {

	// If folder is created should be added to watch list
	// Will need to send attr back
	if event.Op&fsnotify.Create == fsnotify.Create {
	} else if event.Op&fsnotify.Write == fsnotify.Write {
		// If Write happened then will need to update cache with write

		payload := &AttrUpdateInfo{}

		info, err := os.Stat(event.Name)

		if err == nil {

			zap.L().Debug("Got Watch Event",
				zap.String("op", "write"),
				zap.String("path", event.Name),
				zap.Int64("size", info.Size()),
				zap.String("mode", info.Mode().String()),
				zap.Time("mtime", info.ModTime()),
			)

			payload.Path = event.Name
			payload.Size = info.Size()
			payload.Mode = info.Mode()
			payload.ModTime = info.ModTime().UnixNano()

			pkt := &Packet{
				Op:    AttrUpdateRequest,
				Flags: 0,
				Data:  payload,
			}

			AgentTalker().SendPacket(pkt)

		} else {
			zap.L().Warn("Stat Failed",
				zap.String("op", "write"),
				zap.String("path", event.Name),
			)
		}

	} else if event.Op&fsnotify.Remove == fsnotify.Remove {
		// If Deleted then will need to be sent back
	} else if event.Op&fsnotify.Rename == fsnotify.Rename {

	} else if event.Op&fsnotify.Chmod == fsnotify.Chmod {
		// Simple Attr Update

		payload := &AttrUpdateInfo{}

		info, err := os.Stat(event.Name)

		if err == nil {

			zap.L().Debug("Got Watch Event",
				zap.String("op", "chmod"),
				zap.String("path", event.Name),
				zap.Int64("size", info.Size()),
				zap.String("mode", info.Mode().String()),
				zap.Time("mtime", info.ModTime()),
			)

			payload.Path = event.Name
			payload.Size = info.Size()
			payload.Mode = info.Mode()
			payload.ModTime = info.ModTime().UnixNano()

			pkt := &Packet{
				Op:    AttrUpdateRequest,
				Flags: 0,
				Data:  payload,
			}

			AgentTalker().SendPacket(pkt)

		} else {
			zap.L().Warn("Stat Failed",
				zap.String("op", "chmod"),
				zap.String("path", event.Name),
			)
		}
	}

}

func (w *watcher) watchDir(dirPath string) error {
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

		zap.L().Debug("Watching Dir",
			zap.String("dir", dir),
		)

		w.watcher.Add(dir)
	}

	return nil
}

func (w *watcher) WatchPaths(request *Packet) error {

	watchInfo := request.Data.(*WatchInfo)

	zap.L().Debug("Processing Watch Request",
		zap.String("op", "watchdir"),
		zap.Uint8("conn_id", request.ConnId),
		zap.Bool("request", request.IsRequest()),
		zap.Uint64("id", request.Id),
		zap.Strings("paths", watchInfo.Paths),
	)

	var err error

	for _, p := range watchInfo.Paths {
		err = w.watchDir(p)

		if err != nil {
			zap.L().Debug("Watch Error Response",
				zap.String("op", "watchdir"),
				zap.Uint8("conn_id", request.ConnId),
				zap.Bool("request", request.IsRequest()),
				zap.Uint64("id", request.Id),
				zap.Strings("paths", watchInfo.Paths),
				zap.Error(err),
			)
			return err
		}
	}

	return nil
}

func (w *watcher) UnwatchPaths() {
	zap.L().Debug("Removing Watch Paths")

	for name := range w.Paths {
		w.watcher.Remove(name)
	}

	w.Paths = make(map[string]bool)
}
