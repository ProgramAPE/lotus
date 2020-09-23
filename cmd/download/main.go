package main

import (
	"flag"
	"fmt"
	"github.com/filecoin-project/lotus/extern/sector-storage/ffiwrapper"
	"io"
)

func main() {
	conf := flag.String("c", "conf.toml", "download config")
	file := flag.String("d", "", "file name")
	flag.Parse()
	err := ffiwrapper.InitQiniu(*conf)
	if err != nil {
		fmt.Println(err)
		return
	}
	f, err := ffiwrapper.DownloadFile(*file)
	if err != nil {
		fmt.Println(err)
	}
	n, err := f.Seek(0, io.SeekEnd)
	fmt.Println("file end", n, err)
	f.Close()
}