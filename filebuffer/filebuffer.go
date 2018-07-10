package filebuffer

import (
	"sync"
	"time"

	"github.com/m-lab/pusher/fileinfo"
	"github.com/m-lab/pusher/tarcache"
)

type fileGroup struct {
	members []*fileinfo.LocalDataFile
	maxMtime  time.Time
}

type FileBuffer struct {
	mutex sync.Mutex
	minFileAge time.Duration
	fileGroupHashFn func(string) string
	fileGroups map[string]*fileGroup
	tarCache *tarcache.TarCache
}

func New(minFileAge time.Duration, fileGroupHashFn func(string) string, tarCache *tarcache.TarCache) *FileBuffer {
	return &FileBuffer{
		minFileAge: minFileAge,
		fileGroupHashFn: fileGroupHashFn,
		tarCache: tarCache,
	}
}

func (b *FileBuffer) Add(file *fileinfo.LocalDataFile) {
}
