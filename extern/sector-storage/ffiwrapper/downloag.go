package ffiwrapper

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pelletier/go-toml"
)

type qConfig struct {
	TempPath string   `json:"temp_path" toml:"temp_path"`
	Uid      uint64   `json:"uid" toml:"uid"`
	IoHosts  []string `json:"io_hosts" toml:"io_hosts"`
	Bucket   string   `json:"bucket" toml:"bucket"`
}

var qconf *qConfig

func InitQiniu(confPath string) error {
	var configuration qConfig
	raw, err := ioutil.ReadFile(confPath)
	if err != nil {
		return err
	}
	ext := path.Ext(confPath)
	ext = strings.ToLower(ext)
	if ext == ".json" {
		err = json.Unmarshal(raw, &configuration)
	} else if ext == ".toml" {
		err = toml.Unmarshal(raw, &configuration)
	} else {
		return errors.New("configuration format invalid")
	}
	qconf = &configuration
	return err
}

func DownloadFile(path string) (f *os.File, err error) {
	for i := 0; i < 3; i++ {
		f, err = downloadFileInner(path)
		if err == nil {
			return
		}
	}
	return
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func downloadFileInner(path string) (*os.File, error) {
	if qconf == nil {
		return nil, errors.New("qiniu is not init")
	}
	remotePath := path
	if strings.HasPrefix(path, "/") {
		remotePath = strings.TrimPrefix(remotePath, "/")
	}
	localPath := filepath.Join(qconf.TempPath, strings.Replace(remotePath, "/", "-", -1))
	f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	length, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	rnd := rand.Uint32()
	host := qconf.IoHosts[rnd%uint32(len(qconf.IoHosts))]

	fmt.Println("remote path", remotePath)
	url := fmt.Sprintf("%s/getfile/%d/%s/%s", host, qconf.Uid, qconf.Bucket, remotePath)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if length != 0 {
		r := fmt.Sprintf("bytes=%d-", length)
		req.Header.Set("Range", r)
		fmt.Println("continue download")
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return f, nil
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent{
		return nil, errors.New(response.Status)
	}
	ctLength := response.ContentLength

	n, err := io.Copy(f, response.Body)
	if err != nil {
		return nil, err
	}
	if ctLength != n {
		log.Warn("download length not equal", ctLength, n)
	}
	f.Seek(0, io.SeekStart)
	return f, nil
}
