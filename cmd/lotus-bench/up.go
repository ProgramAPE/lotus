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
	submitPath(reqs)
}

func submitPath(paths []*req) {
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

type req struct {
	Path string `json:"path"`
}

func newReq(s string) *req {
	return &req{
		Path: s,
	}
}
