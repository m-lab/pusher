package pusher

import (
	"time"
	"github.com/m-lab/pusher/bytecount"
	"github.com/m-lab/pusher/filebuffer"
	"github.com/m-lab/pusher/fileinfo"
	"github.com/m-lab/pusher/tarcache"
	"github.com/m-lab/pusher/uploader"
)


// TrivialHash is a hash function that does nothing.  For all pusher configs
// for experiments that do not need to ensure that particular files are grouped
// together, use the TrivialHash for the FileGroupHashFn.
func TrivialHash(s string) string {
	return s
}

// Config holds all config information needed for a new pusher channel.
type Config struct {
	Project, Bucket, Creds string
	TarfileSizeThreshold bytecount.ByteCount
	MinFileUploadAge time.Duration
	FileGroupHashFn func(string)string
}

// New creates a new channel to which files for upload should be sent, and also
// sets up the goroutine which does the tar and push operations.
func New(config Config) chan *fileinfo.LocalDataFile {
	// Set up the processing chain.
	uploader := uploader.New(config.Project, config.Bucket, config.Creds)
	tarCache := tarcache.New(config.TarfileSizeThreshold, uploader)
	fileBuffer := filebuffer.New(config.MinFileUploadAge, config.FileGroupHashFn, tarCache)

	// By giving this channel a large buffer, we attempt to decouple file
	// discovery event response times from any file processing times.
	c := make(chan *fileinfo.LocalDataFile, 1000000)
	go func() {
		for file := range c {
			fileBuffer.Add(file)
		}
	}()
	return c
}
