package sectorstorage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
)

type sectorFile struct {
	sid *abi.SectorID
	size int64
}

func addCheckList(lp stores.SectorPaths, sid *abi.SectorID, ssize abi.SectorSize, checkList map[string]*sectorFile) {
	checkList[lp.Sealed] = &sectorFile{
		sid:  sid,
		size: int64(ssize),
	}
	checkList[filepath.Join(lp.Cache, "t_aux")] = &sectorFile{
		sid:  sid,
		size: 0,
	}
	checkList[filepath.Join(lp.Cache, "p_aux")] = &sectorFile{
		sid:  sid,
		size: 0,
	}
	addCacheFilePathsForSectorSize(checkList, lp.Cache, ssize, sid)
}

func fileCount(ssize abi.SectorSize) int{
	c := 3
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		c+= 1
	case 32 << 30:
		c+=8
	case 64 << 30:
		c+=16
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
	return c
}

type fileStat struct {
	Name string `json:"name"`
	size int64 `json:"size"`
}

func insert(bad *[]abi.SectorID, sid *abi.SectorID)  {
	for _, v := range *bad{
		if v.Miner == (*sid).Miner && v.Number == (*sid).Number {
			return
		}
	}
	*bad = append(*bad, *sid)
}

func checkBad(bad *[]abi.SectorID, checkList map[string]*sectorFile){
	list := getKeys(checkList)
	up :=  os.Getenv("UP_MONITOR")
	if up == "" {
		return
	}
	s, _ := json.Marshal(list)
	sr := bytes.NewReader(s)
	r, err := http.DefaultClient.Post(up + "/stat", "application/json", sr)
	if err != nil {
		log.Warnf("submit path count %d, err %s\n", len(list), err.Error())
		return
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		log.Warnf("submit path count %d code %d\n", len(list), r.StatusCode)
		return
	}
	var fList []fileStat
	j := json.NewDecoder(r.Body)
	err = j.Decode(&fList)
	if err != nil {
		log.Warnf("decode path count %d, err %s\n", len(list), err.Error())
		return
	}

	for _, v := range fList {
		p := checkList["/" + v.Name]
		if p == nil {
			log.Warn("no file!!!", "/" + v.Name)
		}
		if v.size == -1 { // not found
			log.Warn("file is not exit", "/" + v.Name, p.sid.Number, p.sid.Miner)
			insert(bad, p.sid)
		} else if p.size != 0 && p.size != v.size {
			log.Warn("file size is wrong", p.size, v.size, "/" +v.Name, p.sid.Number, p.sid.Miner)
			insert(bad, p.sid)
		}
	}
}

func addCacheFilePathsForSectorSize(checkList map[string]*sectorFile, cacheDir string, ssize abi.SectorSize, sid *abi.SectorID) {
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		checkList[filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat")] = &sectorFile{
			sid:  sid,
			size: 0,
		}
	case 32 << 30:
		for i := 0; i < 8; i++ {
			checkList[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = &sectorFile{
				sid:  sid,
				size: 0,
			}
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			checkList[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = &sectorFile{
				sid:  sid,
				size: 0,
			}
		}
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
}

func getKeys(m map[string]*sectorFile) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k[1:])
	}
	return keys
}