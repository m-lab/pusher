// Package tarcache supports the creation and running of a pipeline that
// receives files, tars up the contents, and uploads everything when the tarfile
// is big enough or the contents are old enough.
package tarcache

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/m-lab/go/bytecount"
	"github.com/m-lab/pusher/filename"
	"github.com/m-lab/pusher/tarfile"
	"github.com/m-lab/pusher/uploader"
)

var (
	pusherTarfilesUploadCalls = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_tarfiles_upload_calls_total",
			Help: "The number of times upload has been called",
		},
		[]string{"datatype", "reason"},
	)
	pusherStrangeFilenames = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_strange_filenames_total",
			Help: "The number of files we have seen with names that looked surprising in some way",
		},
		[]string{"datatype"})
	pusherFileOpenErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_file_open_errors_total",
			Help: "The number of times we could not open a file that we were trying to add to the tarfile",
		},
		[]string{"datatype"})
)

// TarCache contains everything you need to incrementally create a tarfile.
// Once enough time has passed since the first file was added OR the resulting
// tar file has become big enough, it will call the uploadAndDelete() method.
// To upload a lot of tarfiles, you should only have to create one TarCache.
// The TarCache takes care of creating each tarfile and getting it uploaded.
type TarCache struct {
	fileChannel    <-chan filename.System
	timeoutChannel chan string
	currentTarfile map[string]tarfile.Tarfile
	sizeThreshold  bytecount.ByteCount
	ageThreshold   time.Duration
	rootDirectory  filename.System
	uploader       uploader.Uploader
	datatype       string
}

// New creates a new TarCache object and returns a pointer to it and the
// channel used to send data to the TarCache.
func New(rootDirectory filename.System, datatype string, sizeThreshold bytecount.ByteCount, ageThreshold time.Duration, uploader uploader.Uploader) (*TarCache, chan<- filename.System) {
	if !strings.HasSuffix(string(rootDirectory), "/") {
		rootDirectory = filename.System(string(rootDirectory) + "/")
	}
	// By giving the channel a large buffer, we attempt to decouple file
	// discovery event response times from any file processing times.
	fileChannel := make(chan filename.System, 1000000)
	tarCache := &TarCache{
		fileChannel:    fileChannel,
		timeoutChannel: make(chan string),
		rootDirectory:  rootDirectory,
		currentTarfile: make(map[string]tarfile.Tarfile),
		sizeThreshold:  sizeThreshold,
		ageThreshold:   ageThreshold,
		uploader:       uploader,
		datatype:       datatype,
	}
	return tarCache, fileChannel
}

// ListenForever waits for new files and then uploads them. Using this approach
// allows us to ensure that all file processing happens in this single thread,
// no matter whether the processing is happening due to age thresholds or size
// thresholds.
func (t *TarCache) ListenForever(ctx context.Context) {
	for {
		select {
		case subdir := <-t.timeoutChannel:
			t.uploadAndDelete(subdir)
			pusherTarfilesUploadCalls.WithLabelValues(t.datatype, "age_threshold_met").Inc()
		case dataFile, channelOpen := <-t.fileChannel:
			if channelOpen {
				t.add(dataFile)
			} else {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (t *TarCache) makeTimer(subdir string) *time.Timer {
	log.Println("Starting timer for " + subdir)
	return time.AfterFunc(t.ageThreshold, func() {
		t.timeoutChannel <- subdir
	})
}

// Add adds the contents of a file to the underlying tarfile.  It possibly
// calls uploadAndDelete() afterwards.
func (t *TarCache) add(fname filename.System) {
	internalName := fname.Internal(t.rootDirectory)
	if warning := internalName.Lint(); warning != nil {
		log.Println("Strange filename encountered:", warning)
		pusherStrangeFilenames.WithLabelValues(t.datatype).Inc()
	}
	file, err := os.Open(string(fname))
	if err != nil {
		pusherFileOpenErrors.WithLabelValues(t.datatype).Inc()
		log.Printf("Could not open %s (error: %q)\n", fname, err)
		return
	}
	subdir := internalName.Subdir()
	if _, ok := t.currentTarfile[subdir]; !ok {
		t.currentTarfile[subdir] = tarfile.New(filename.System(subdir), t.datatype)
	}
	tf := t.currentTarfile[subdir]
	tf.Add(internalName, file, t.makeTimer)
	if tf.Size() > t.sizeThreshold {
		pusherTarfilesUploadCalls.WithLabelValues(t.datatype, "size_threshold_met").Inc()
		t.uploadAndDelete(subdir)
	}
}

// Upload the buffer, delete the component files, start a new buffer.
func (t *TarCache) uploadAndDelete(subdir string) {
	if tf, ok := t.currentTarfile[subdir]; ok {
		tf.UploadAndDelete(t.uploader)
		delete(t.currentTarfile, subdir)
	} else {
		log.Printf("Upload called for nonexistent tarfile for directory %q\n", subdir)
	}
}
