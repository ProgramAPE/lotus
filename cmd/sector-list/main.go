package main

import (
	"flag"
	"log"
	"path"
	"strconv"
	"strings"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
)

func main()  {
	c := flag.String("c", "config.toml", "config file")
	root := flag.String("p", "home/fc", "prefix path")
	flag.Parse()

	conf, err := operation.Load(*c)
	if err != nil {
		log.Println("load conf failed", err, *c)
		return
	}

	l := operation.NewLister(conf)
	f:= l.ListPrefix(*root)
	log.Println(f)


	sectors := loadSectors(f)
	log.Println(sectors)
}

func loadSectors(file []string)(ret []abi.SectorID) {
	for _, v := range file {
		x := parseSector(v)
		if x != nil {
			ret = append(ret, *x)
		}
	}
	return
}

func parseSector(file string) *abi.SectorID {
	f := path.Base(file)
	parts := strings.Split(f, "-")
	if len(parts) != 3 {
		log.Println("Invalid file", f)
		return nil
	}
	sectorId, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		log.Println("Invalid file", f, err)
		return nil
	}
	s := parts[1]
	if len(s) < 3 {
		log.Println("Invalid file", f, s)
		return nil
	}
	miner, err := strconv.ParseUint(s[1:], 10, 64)
	if err != nil {
		log.Println("Invalid file", f, err)
		return nil
	}
	return &abi.SectorID{
		Miner:  abi.ActorID(miner),
		Number: abi.SectorNumber(sectorId),
	}
}
