package pusher

import (
	"github.com/m-lab/pusher/bytecount"
	"github.com/m-lab/pusher/fileinfo"
	"github.com/m-lab/pusher/tarcache"
	"github.com/m-lab/pusher/uploader"
	"time"
)

// Config holds all config information needed for a new pusher channel.
type Config struct {
	Project, Bucket      string
	TarfileSizeThreshold bytecount.ByteCount
	FileAgeThreshold     time.Duration
}

// New creates a new channel to which files for upload should be sent, and also
// sets up the goroutine which does the tar and push operations.
func New(config Config) chan *fileinfo.LocalDataFile {
	// Set up the processing chain.
	uploader := uploader.New(config.Project, config.Bucket)
	tarCache := tarcache.New(config.TarfileSizeThreshold, config.FileAgeThreshold, uploader)

	// By giving this channel a large buffer, we attempt to decouple file
	// discovery event response times from any file processing times.
	c := make(chan *fileinfo.LocalDataFile, 1000000)
	go func() {
		for file := range c {
			tarCache.Add(file)
		}
	}()
	return c
}
