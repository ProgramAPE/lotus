package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper/basicfs"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
)

func lastTreePaths(cacheDir string) []string {
	var ret []string
	paths, err := ioutil.ReadDir(cacheDir)
	if err != nil {
		return []string{}
	}
	for _, v := range paths {
		if !v.IsDir() {
			if strings.Contains(v.Name(), "tree-r-last") ||
				v.Name() == "p_aux" || v.Name() == "t_aux"{
				ret = append(ret, path.Join(cacheDir, v.Name()))
			}
		}
	}
	return ret
}

func submitQ(sbfs *basicfs.Provider, sector abi.SectorID) {
	cache := filepath.Join(sbfs.Root, stores.FTCache.String(), stores.SectorName(sector))
	seal := filepath.Join(sbfs.Root, stores.FTSealed.String(), stores.SectorName(sector))

	pathList := lastTreePaths(cache)
	pathList = append(pathList, seal)
	var reqs []*req
	for _, path := range pathList {
		fmt.Println("path ", path)
		reqs = append(reqs, newReq(path))
	}
	submitPaths(reqs)
}

func submitPathOut(paths []*req) {
	up :=  os.Getenv("UP_MONITOR")

	if up == "" {
		return
	}
	s, _ := json.Marshal(paths)
	sr := bytes.NewReader(s)
	r, err := http.DefaultClient.Post(up, "application/json", sr)
	if err != nil {
		fmt.Printf("submit path %+v err %s\n", paths, err.Error())
	} else {
		fmt.Printf("submit path %+v code %d\n", paths, r.StatusCode)
	}
}

var g_conf *operation.Config

var confLock sync.Mutex

func getConf() *operation.Config {
	up :=  os.Getenv("QINIU")
	if up == "" {
		return nil
	}
	confLock.Lock()
	defer confLock.Unlock()
	if g_conf != nil {
		return g_conf
	}
	c, err := operation.Load(up)
	if err != nil {
		return nil
	}
	g_conf = c
	WatchConfig(up)
	return c
}

func WatchConfig(filename string) {
	initWG := sync.WaitGroup{}
	initWG.Add(1)
	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Fatal(err)
		}
		defer watcher.Close()

		configFile := filepath.Clean(filename)
		configDir, _ := filepath.Split(configFile)
		realConfigFile, _ := filepath.EvalSymlinks(filename)

		eventsWG := sync.WaitGroup{}
		eventsWG.Add(1)
		go func() {
			for {
				select {
				case event, ok := <-watcher.Events:
					if !ok { // 'Events' channel is closed
						eventsWG.Done()
						return
					}
					currentConfigFile, _ := filepath.EvalSymlinks(filename)
					// we only care about the config file with the following cases:
					// 1 - if the config file was modified or created
					// 2 - if the real path to the config file changed (eg: k8s ConfigMap replacement)
					const writeOrCreateMask = fsnotify.Write | fsnotify.Create
					if (filepath.Clean(event.Name) == configFile &&
						event.Op&writeOrCreateMask != 0) ||
						(currentConfigFile != "" && currentConfigFile != realConfigFile) {
						realConfigFile = currentConfigFile
						c, err:= operation.Load(realConfigFile)
						fmt.Printf("re reading config file: error %v\n", err)
						g_conf = c

					} else if filepath.Clean(event.Name) == configFile &&
						event.Op&fsnotify.Remove&fsnotify.Remove != 0 {
						eventsWG.Done()
						return
					}

				case err, ok := <-watcher.Errors:
					if ok { // 'Errors' channel is not closed
						fmt.Printf("watcher error: %v\n", err)
					}
					eventsWG.Done()
					return
				}
			}
		}()
		watcher.Add(configDir)
		initWG.Done()   // done initializing the watch in this go routine, so the parent routine can move on...
		eventsWG.Wait() // now, wait for event loop to end in this go-routine...
	}()
	initWG.Wait() // make sure that the go routine above fully ended before returning
}

func submitPaths(paths []*req) {
	up :=  os.Getenv("QINIU")

	if up == "" {
		return
	}
	conf2 := getConf()
	if conf2 == nil {
		log.Error("load config error")
		return
	}
	if conf2.Sim {
		submitPathOut(paths)
		return
	}
	uploader := operation.NewUploader(conf2)
	for _, v := range paths {
		err := uploader.Upload(v.Path, v.Path)
		fmt.Printf("submit path %+v err %s\n", v.Path, err)
	}
}

type req struct {
	Path string `json:"path"`
}

func newReq(s string) *req {
	return &req{
		Path: s,
	}
}
