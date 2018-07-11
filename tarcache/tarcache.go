package tarcache

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"log"
	"math/rand"
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
	mutex          sync.Mutex
	currentTarfile *tarfile
	sizeThreshold  bytecount.ByteCount
	ageThreshold   time.Duration
	rootDirectory  string
	uploader       uploader.Uploader
}

// A tarfile represents a single tar file containing data for upload
type tarfile struct {
	timer      *time.Timer
	members    []*fileinfo.LocalDataFile
	contents   *bytes.Buffer
	tarWriter  *tar.Writer
	gzipWriter *gzip.Writer
	uploaded   bool
}

// New creates a new TarCache object and returns a pointer to it.  The TarCache
// will have its associated timeOutLoop running.  There is no way to stop this
// timeout loop outside of the tarcache  The TarCache will have its associated
// timeOutLoop running.  There is no way to stop this timeout loop outside of
// the tarcache package.
func New(rootDirectory string, sizeThreshold bytecount.ByteCount, ageThreshold time.Duration, uploader uploader.Uploader) *TarCache {
	if !strings.HasSuffix(rootDirectory, "/") {
		rootDirectory += "/"
	}
	tarCache := &TarCache{
		rootDirectory:  rootDirectory,
		currentTarfile: newTarfile(),
		sizeThreshold:  sizeThreshold,
		ageThreshold:   ageThreshold,
		uploader:       uploader,
	}
	return tarCache
}

func newTarfile() *tarfile {
	buffer := new(bytes.Buffer)
	gzipWriter := gzip.NewWriter(buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	return &tarfile{
		uploaded:   false,
		contents:   buffer,
		tarWriter:  tarWriter,
		gzipWriter: gzipWriter,
	}
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
	tf := t.currentTarfile
	if err = tf.tarWriter.WriteHeader(header); err != nil {
		log.Printf("Could not write the tarfile header for %s (error: %q)\n", file.AbsoluteFileName, err)
		t.flushWhileLocked()
		return
	}
	if _, err = tf.tarWriter.Write(contents); err != nil {
		log.Printf("Could not write the tarfile contents for %s (error: %q)\n", file.AbsoluteFileName, err)
		t.flushWhileLocked()
		return
	}
	if err = tf.tarWriter.Flush(); err != nil {
		log.Printf("Could not flush the tarWriter (error: %q)\n", err)
		t.flushWhileLocked()
		return
	}
	if err = tf.gzipWriter.Flush(); err != nil {
		log.Printf("Could not flush the gzipWriter (error: %q)\n", err)
		t.flushWhileLocked()
		return
	}
	if len(tf.members) == 0 {
		tf.timer = time.AfterFunc(t.ageThreshold, t.flushAfterTimeout)
	}
	tf.members = append(tf.members, file)
	if bytecount.ByteCount(tf.contents.Len()) > t.sizeThreshold {
		t.flushWhileLocked()
	}
}

func (t *TarCache) flushAfterTimeout() {
	log.Println("Flush called after timeout")
	t.Flush()
}

// Flush the buffer.
func (t *TarCache) Flush() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.flushWhileLocked()
}

// Only call this function when holding the mutex lock.
func (t *TarCache) flushWhileLocked() {
	t.currentTarfile.uploadAndDelete(t.uploader)
	t.currentTarfile = newTarfile()
}

// Upload the contents of the tarfile and then delete the component files.
func (t *tarfile) uploadAndDelete(uploader uploader.Uploader) {
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
	if len(t.members) == 0 || t.uploaded {
		return
	}
	t.tarWriter.Close()
	t.gzipWriter.Close()
	backoff := time.Duration(100) * time.Millisecond
	for err := uploader.Upload(t.contents); err != nil; err = uploader.Upload(t.contents) {
		log.Printf("Error uploading: %q, will retry after %s\n", err, backoff.String())
		time.Sleep(backoff)
		backoff = time.Duration(backoff.Seconds()*2) * time.Second
		// The maximum retry interval is every five minutes. Once five minutes has
		// been reached, wait for five minutes plus a random number of seconds.
		if backoff.Minutes() > 5 {
			backoff = time.Duration(300+(rand.Int()%60)) * time.Second
		}
	}
	t.uploaded = true
	for _, file := range t.members {
		log.Printf("Removing %s\n", file.AbsoluteFileName)
		err := os.Remove(file.AbsoluteFileName)
		if err != nil {
			log.Printf("Failed to remove %s (error: %q)\n", file.AbsoluteFileName, err)
		}
	}
}
