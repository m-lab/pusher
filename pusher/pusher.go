package pusher

import (
	"time"

	"github.com/m-lab/pusher/bytecount"
	"github.com/m-lab/pusher/fileinfo"
	"github.com/m-lab/pusher/tarcache"
	"github.com/m-lab/pusher/uploader"
)

// Config holds all config information needed for a new pusher channel.
type Config struct {
	Project, Bucket, Directory string
	TarfileSizeThreshold       bytecount.ByteCount
	FileAgeThreshold           time.Duration
}

// New creates a new channel to which files for upload should be sent, and also
// sets up the goroutine which does the tar and push operations.
func New(config Config) chan<- *fileinfo.LocalDataFile {
	// Set up the processing chain.
	uploader := uploader.New(config.Project, config.Bucket)
	tarCache, channel := tarcache.New(config.Directory, config.TarfileSizeThreshold, config.FileAgeThreshold, uploader)
	go tarCache.ListenForever()
	return channel
}
