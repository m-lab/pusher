package tarcache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"log"
	"os"
	"time"

	"github.com/m-lab/pusher/backoff"
	"github.com/m-lab/pusher/uploader"
)

// A tarfile represents a single tar file containing data for upload
type tarfile struct {
	timeout    *time.Timer
	members    []LocalDataFile
	memberSet  map[LocalDataFile]struct{}
	contents   *bytes.Buffer
	tarWriter  *tar.Writer
	gzipWriter *gzip.Writer
	subdir     string
}

func newTarfile(dir string) *tarfile {
	pusherTarfilesCreated.Inc()
	// TODO: profile and determine if preallocation is a good idea.
	buffer := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	// If you create more than one tarfile, then these gauges will get confused and confusing.
	pusherCurrentTarfileFileCount.Set(0)
	pusherCurrentTarfileSize.Set(0)
	return &tarfile{
		contents:   buffer,
		tarWriter:  tarWriter,
		gzipWriter: gzipWriter,
		memberSet:  make(map[LocalDataFile]struct{}),
		subdir:     dir,
	}
}

// Upload the contents of the tarfile and then delete the component files. This
// function will never return unsuccessfully. If there are files to upload, this
// method will keep trying until the upload succeeds.
func (t *tarfile) uploadAndDelete(uploader uploader.Uploader) {
	if len(t.members) == 0 {
		pusherEmptyUploads.Inc()
		pusherSuccessTimestamp.SetToCurrentTime()
		log.Println("uploadAndDelete called on an empty tarfile.")
		return
	}
	if t.timeout != nil {
		t.timeout.Stop()
	}
	t.tarWriter.Close()
	t.gzipWriter.Close()
	pusherFilesPerTarfile.Observe(float64(len(t.members)))
	pusherBytesPerTarfile.Observe(float64(t.contents.Len()))
	bytes := t.contents.Bytes()
	// Try to upload until the upload succeeds.
	backoff.Retry(
		func() error {
			return uploader.Upload(t.subdir, bytes)
		},
		time.Duration(100)*time.Millisecond,
		time.Duration(5)*time.Minute,
		"upload",
	)
	pusherTarfilesUploaded.Inc()
	pusherSuccessTimestamp.SetToCurrentTime()
	for _, file := range t.members {
		// If the file can't be removed, then it either was already removed or the
		// remove call failed for some unknown reason (permissions, maybe?). If the
		// file still exists after this attempted remove, then it should eventually
		// get picked up by the finder.
		if err := os.Remove(string(file)); err == nil {
			pusherFilesRemoved.Inc()
		} else {
			pusherFileRemoveErrors.Inc()
			log.Printf("Failed to remove %v (error: %q)\n", file, err)
		}
	}
}
