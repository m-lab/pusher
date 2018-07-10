package tarcache

import (
	"bytes"
	"sync"
	"time"

	"github.com/m-lab/pusher/bytecount"
	"github.com/m-lab/pusher/fileinfo"
	"github.com/m-lab/pusher/uploader"
)

type TarCache struct {
	mutex sync.Mutex
	members []*fileinfo.LocalDataFile
	tarFileContents *bytes.Buffer
	sizeThreshold bytecount.ByteCount
	ageThreshold time.Duration
	uploader *uploader.Uploader
}

func New(sizeThreshold bytecount.ByteCount, ageThreshold time.Duration, uploader *uploader.Uploader) *TarCache {
	return &TarCache{
		tarFileContents: new(bytes.Buffer),
		sizeThreshold: sizeThreshold,
		ageThreshold: ageThreshold,
		uploader: uploader,
	}
}

func (t *TarCache) Add(file *fileinfo.LocalDataFile) {
}
