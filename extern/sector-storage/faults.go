package sectorstorage

import (
	"context"
	"fmt"
	"path/filepath"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

// FaultTracker TODO: Track things more actively
type FaultTracker interface {
	CheckProvable(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storage.SectorRef) ([]abi.SectorID, error)
}

// CheckProvable returns unprovable sectors
func (m *Manager) CheckProvable(ctx context.Context, pp abi.RegisteredPoStProof, sectors []storage.SectorRef) ([]abi.SectorID, error) {
	var bad []abi.SectorID

	ssize, err := pp.SectorSize()
	if err != nil {
		return nil, err
	}

	checkList := make(map[string]SectorFile, len(sectors) * fileCount(ssize)*2)
	// TODO: More better checks
	for _, sector := range sectors {
		err := func() error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			locked, err := m.index.StorageTryLock(ctx, sector.ID, storiface.FTSealed|storiface.FTCache, storiface.FTNone)
			if err != nil {
				return xerrors.Errorf("acquiring sector lock: %w", err)
			}

			if !locked {
				log.Warnw("CheckProvable Sector FAULT: can't acquire read lock", "sector", sector, "sealed")
				bad = append(bad, sector.ID)
				return nil
			}

			lp, _, err := m.localStore.AcquireSector(ctx, sector, storiface.FTSealed|storiface.FTCache, storiface.FTNone, storiface.PathStorage, storiface.AcquireMove)
			if err != nil {
				log.Warnw("CheckProvable Sector FAULT: acquire sector in checkProvable", "sector", sector, "error", err)
				bad = append(bad, sector.ID)
				return nil
			}

			if lp.Sealed == "" || lp.Cache == "" {
				log.Warnw("CheckProvable Sector FAULT: cache an/or sealed paths not found", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache)
				bad = append(bad, sector.ID)
				return nil
			}

			addCheckList(lp, sector.ID, ssize, checkList)

			//toCheck := map[string]int64{
			//	lp.Sealed:                        1,
			//	filepath.Join(lp.Cache, "t_aux"): 0,
			//	filepath.Join(lp.Cache, "p_aux"): 0,
			//}
			//
			//addCachePathsForSectorSize(toCheck, lp.Cache, ssize)
			//
			//for p, sz := range toCheck {
			//	st, err := os.Stat(p)
			//	if err != nil {
			//		log.Warnw("CheckProvable Sector FAULT: sector file stat error", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "file", p, "err", err)
			//		bad = append(bad, sector.ID)
			//		return nil
			//	}
			//
			//	if sz != 0 {
			//		if st.Size() != int64(ssize)*sz {
			//			log.Warnw("CheckProvable Sector FAULT: sector file is wrong size", "sector", sector, "sealed", lp.Sealed, "cache", lp.Cache, "file", p, "size", st.Size(), "expectSize", int64(ssize)*sz)
			//			bad = append(bad, sector.ID)
			//			return nil
			//		}
			//	}
			//}

			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	checkBad(&bad, checkList)
	return bad, nil
}

func addCachePathsForSectorSize(chk map[string]int64, cacheDir string, ssize abi.SectorSize) {
	switch ssize {
	case 2 << 10:
		fallthrough
	case 8 << 20:
		fallthrough
	case 512 << 20:
		chk[filepath.Join(cacheDir, "sc-02-data-tree-r-last.dat")] = 0
	case 32 << 30:
		for i := 0; i < 8; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	case 64 << 30:
		for i := 0; i < 16; i++ {
			chk[filepath.Join(cacheDir, fmt.Sprintf("sc-02-data-tree-r-last-%d.dat", i))] = 0
		}
	default:
		log.Warnf("not checking cache files of %s sectors for faults", ssize)
	}
}

var _ FaultTracker = &Manager{}
