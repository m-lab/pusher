// Package tarcache supports the creation and running of a pipeline that
// receives files, tars up the contents, and uploads everything when the tarfile
// is big enough or the contents are old enough.
package tarcache

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/memoryless"
	"github.com/m-lab/go/rtx"

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
	ageThreshold   memoryless.Config
	fileRatio      float64 // Ratio of individual files to be added to the tarcache [0, 1].
	rootDirectory  filename.System
	uploader       uploader.Uploader
	datatype       string
	metadata       *flagx.KeyValue
}

// New creates a new TarCache object and returns a pointer to it and the
// channel used to send data to the TarCache.
func New(rootDirectory filename.System, datatype string, ratio float64, metadata *flagx.KeyValue, sizeThreshold bytecount.ByteCount, ageThreshold memoryless.Config, uploader uploader.Uploader) (*TarCache, chan<- filename.System) {
	rtx.Must(ageThreshold.Check(), "Bad config for the ageThreshold")
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
		fileRatio:      ratio,
		uploader:       uploader,
		datatype:       datatype,
		metadata:       metadata,
	}
	return tarCache, fileChannel
}

// ListenForever waits for new files and then uploads them. Using this approach
// allows us to ensure that all file processing happens in this single thread,
// no matter whether the processing is happening due to age thresholds or size
// thresholds.
func (t *TarCache) ListenForever(termCtx context.Context, killCtx context.Context) {
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
		case <-termCtx.Done():
			t.uploadAll()
		case <-killCtx.Done():
			t.uploadAll()
			return
		}
	}
}

func (t *TarCache) uploadAll() {
	// Upload everything in parallel on an emergency basis.
	wg := sync.WaitGroup{}

	// Make a copy of the list of subdirectories because uploadAndDelete modifies
	// the t.currentTarfile map.
	currentTarfiles := []string{}
	for subdir := range t.currentTarfile {
		currentTarfiles = append(currentTarfiles, subdir)
	}

	// We can't use tarcache.UploadAndDelete in this loop without adding mutexes to
	// all accesses and modifications of t.currentTarfile or uploading all these
	// tarfiles in series rather than in parallel. Adding mutexes to all accesses
	// seems like overkill because everything else in a tarcache is
	// single-threaded; Uploading tarfiles in series seems contrary to the idea
	// that uploadAll is called on an emergency basis.
	for _, subdirTarfile := range t.currentTarfile {
		wg.Add(1)
		go func(tf tarfile.Tarfile) {
			pusherTarfilesUploadCalls.WithLabelValues(t.datatype, "emergency_upload").Inc()
			tf.UploadAndDelete(t.uploader)
			wg.Done()
		}(subdirTarfile)
	}
	wg.Wait()

	// After uploading everything, clear the cache.
	t.currentTarfile = make(map[string]tarfile.Tarfile)
}

func (t *TarCache) makeTimer(subdir string) *time.Timer {
	log.Println("Starting timer for " + t.datatype + "/" + subdir)
	timer, err := memoryless.AfterFunc(t.ageThreshold, func() {
		t.timeoutChannel <- subdir
	})
	rtx.Must(err, "This config is supposed to be fine - we already checked it in NewTarCache - this should never happen")
	return timer
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
		t.currentTarfile[subdir] = tarfile.New(filename.System(subdir), t.datatype, t.fileRatio, t.metadata.Get())
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
