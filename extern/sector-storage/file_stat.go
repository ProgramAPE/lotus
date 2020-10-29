package sectorstorage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"

	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
)

type SectorFile struct {
	Sid  abi.SectorID `json:"sid"`
	Size int64        `json:"size"`
}

func CheckSectors(root string, sectors []abi.SectorID, ssize abi.SectorSize) []abi.SectorID {
	var bad []abi.SectorID
	var checkList = make(map[string]SectorFile, len(sectors)*2)
	for _, v := range sectors {
		sealedPath := filepath.Join(root, stores.FTCache.String(), stores.SectorName(v))
		cachePath := filepath.Join(root, stores.FTSealed.String(), stores.SectorName(v))
		addCheckList(stores.SectorPaths{
			ID:     v,
			Cache:  sealedPath,
			Sealed: cachePath,
		}, v, ssize, checkList)
	}

	checkBad(&bad, checkList)
	return bad
}

func addCheckList(lp stores.SectorPaths, sid abi.SectorID, ssize abi.SectorSize, checkList map[string]SectorFile) {
	checkList[lp.Sealed] = SectorFile{
		Sid:  sid,
		Size: int64(ssize),
	}
	checkList[filepath.Join(lp.Cache, "t_aux")] = SectorFile{
		Sid:  sid,
		Size: 0,
	}
	checkList[filepath.Join(lp.Cache, "p_aux")] = SectorFile{
		Sid:  sid,
		Size: 0,
	}

	addCacheFilePathsForSectorSize(checkList, lp.Cache, ssize, sid)
}

func fileCount(ssize abi.SectorSize) int {
	c := 3
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		c += 1
	case 32 << 30:
		c += 8
	case 64 << 30:
		c += 16
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
	return c
}

type fileStat struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func insert(bad *[]abi.SectorID, sid abi.SectorID) {
	for _, v := range *bad {
		if v.Miner == sid.Miner && v.Number == sid.Number {
			return
		}
	}
	*bad = append(*bad, sid)
}

func checkBad(bad *[]abi.SectorID, checkList map[string]SectorFile) error {
	list := getKeys(checkList)
	conf_file := os.Getenv("QINIU")
	if conf_file == "" {
		return nil
	}
	conf, err := operation.Load(conf_file)
	if err != nil {
		log.Error("load conf failed", conf_file, err)
	}
	if conf.Sim {
		return nil
	}
	lister := operation.NewListerV2()
	fList := lister.ListStat(list)
	if fList == nil {
		return errors.New("list stats failed")
	}
	for _, v := range fList {
		//log.Info("file in list", v.Name, v.Size)
		p, ok := checkList["/"+v.Name]
		if !ok {
			fmt.Println("no file!!!", "/"+v.Name)
			continue
		}

		if v.Size == -1 { // not found
			fmt.Println("file is not exist", "/"+v.Name, p.Sid.Number, p.Sid.Miner)
			insert(bad, p.Sid)
		} else if p.Size != 0 && p.Size != v.Size {
			fmt.Println("file size is wrong", p.Size, v.Size, "/"+v.Name, p.Sid.Number, p.Sid.Miner)
			insert(bad, p.Sid)
		}
	}
	return nil
}

func addCacheFilePathsForSectorSize(checkList map[string]SectorFile, cacheDir string, ssize abi.SectorSize, sid abi.SectorID) {
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		checkList[filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat")] = SectorFile{
			Sid:  sid,
			Size: 0,
		}
	case 32 << 30:
		for i := 0; i < 8; i++ {
			checkList[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = SectorFile{
				Sid:  sid,
				Size: 0,
			}
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			checkList[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = SectorFile{
				Sid:  sid,
				Size: 0,
			}
		}
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
}

func getKeys(m map[string]SectorFile) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k[1:])
	}
	return keys
}
