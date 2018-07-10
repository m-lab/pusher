package tarcache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/pusher/bytecount"
	"github.com/m-lab/pusher/fileinfo"
	"github.com/m-lab/pusher/uploader"
)

// TODO: All calls to log.Print* should have corresponding prometheus counters
// that get incremented.

// TarCache contains everything you need to incrementally create a tarfile.
// Once enough time has passed since the first Add() call OR the resulting tar
// file has become big enough, it will call the Upload() method of the passed-in
// uploader.  To upload a lot of tarfiles, you should only have to create one
// TarCache.
type TarCache struct {
	mutex           sync.Mutex
	timer           *time.Timer
	members         []*fileinfo.LocalDataFile
	tarFileContents *bytes.Buffer
	tarWriter       *tar.Writer
	gzipWriter      *gzip.Writer
	sizeThreshold   bytecount.ByteCount
	ageThreshold    time.Duration
	rootDirectory   string
	uploader        *uploader.Uploader
}

// New creates a new TarCache object and returns a pointer to it.  The TarCache
// will have its associated timeOutLoop running.  There is no way to stop this
// timeout loop outside of the tarcache  The TarCache will have its associated
// timeOutLoop running.  There is no way to stop this timeout loop outside of
// the tarcache package.
func New(rootDirectory string, sizeThreshold bytecount.ByteCount, ageThreshold time.Duration, uploader *uploader.Uploader) *TarCache {
	// Technically these next two lines constitute a race condition. In
	// reality they do not, because the ageThreshold is measured in minutes
	// and not microseconds.
	timer := time.NewTimer(ageThreshold)
	timer.Stop()
	buffer := new(bytes.Buffer)
	gzipWriter := gzip.NewWriter(buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	tarCache := &TarCache{
		rootDirectory:   rootDirectory,
		tarFileContents: buffer,
		tarWriter:       tarWriter,
		gzipWriter:      gzipWriter,
		timer:           timer,
		sizeThreshold:   sizeThreshold,
		ageThreshold:    ageThreshold,
		uploader:        uploader,
	}
	go tarCache.runTimeoutLoop()
	return tarCache
}

// Add adds the contents of a file to the underlying tarfile.  It possibly
// calls Upload() afterwards.
func (t *TarCache) Add(file *fileinfo.LocalDataFile) {
	// Reading file contents from the disk could take a while, so don't
	// hold the lock while we do it.
	contents, err := ioutil.ReadFile(file.AbsoluteFileName)
	if err != nil {
		log.Printf("Could not read %s (error: %q)\n", file.AbsoluteFileName, err)
		return
	}
	header := &tar.Header{
		Name: strings.TrimPrefix(file.AbsoluteFileName, t.rootDirectory),
		Mode: 0666,
		Size: int64(len(contents)),
	}

	// All disk reading is done, and now we have to mess with the internals
	// of the TarCache to add the things we read from the disk.  If we ever
	// get an error, we will try to upload whatever we already have and
	// reset everything.
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if err = t.tarWriter.WriteHeader(header); err != nil {
		log.Printf("Could not write the tarfile header for %s (error: %q)\n", file.AbsoluteFileName, err)
		t.flushWhileLocked()
		return
	}
	if _, err = t.tarWriter.Write(contents); err != nil {
		log.Printf("Could not write the tarfile contents for %s (error: %q)\n", file.AbsoluteFileName, err)
		t.flushWhileLocked()
		return
	}
	if err = t.tarWriter.Flush(); err != nil {
		log.Printf("Could not flush the tarWriter (error: %q)\n", err)
		t.flushWhileLocked()
		return
	}
	if err = t.gzipWriter.Flush(); err != nil {
		log.Printf("Could not flush the gzipWriter (error: %q)\n", err)
		t.flushWhileLocked()
		return
	}
	if len(t.members) == 0 {
		t.timer.Reset(t.ageThreshold)
	}
	t.members = append(t.members, file)
	if bytecount.ByteCount(t.tarFileContents.Len()) > t.sizeThreshold {
		t.flushWhileLocked()
	}
}

func (t *TarCache) runTimeoutLoop() {
	for range t.timer.C {
		log.Println("TarCache timeout fired.")
		t.Flush()
	}
}

// Flush the buffer.
func (t *TarCache) Flush() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.flushWhileLocked()
}

// Only call this function when holding the mutex lock.
func (t *TarCache) flushWhileLocked() {
	t.timer.Stop()
	if len(t.members) == 0 {
		return
	}
	t.tarWriter.Close()
	t.gzipWriter.Close()
	if err := t.uploader.Upload(t.tarFileContents); err != nil {
		log.Printf("Error uploading: %q, will retry\n", err)
	}
	for _, file := range t.members {
		err := os.Remove(file.AbsoluteFileName)
		if err != nil {
			log.Printf("Failed to remove %s (error: %q)\n", file.AbsoluteFileName, err)
		}
	}
	t.members = []*fileinfo.LocalDataFile{}
	t.tarFileContents = new(bytes.Buffer)
	t.gzipWriter = gzip.NewWriter(t.tarFileContents)
	t.tarWriter = tar.NewWriter(t.gzipWriter)
}
