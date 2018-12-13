// Package tarcache supports the creation and running of a pipeline that
// receives files, tars up the contents, and uploads everything when the tarfile
// is big enough or the contents are old enough.
package tarcache

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/m-lab/go/bytecount"
	"github.com/m-lab/pusher/tarfile"
	"github.com/m-lab/pusher/uploader"
)

var (
	pusherTarfilesUploadCalls = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_tarfiles_upload_calls_total",
			Help: "The number of times upload has been called",
		},
		[]string{"reason"},
	)
	pusherStrangeFilenames = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_strange_filenames_total",
		Help: "The number of files we have seen with names that looked surprising in some way",
	})
	pusherFileOpenErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_file_open_errors_total",
		Help: "The number of times we could not open a file that we were trying to add to the tarfile",
	})
)

func init() {
	prometheus.MustRegister(pusherTarfilesUploadCalls)
	prometheus.MustRegister(pusherStrangeFilenames)
}

// TarCache contains everything you need to incrementally create a tarfile.
// Once enough time has passed since the first file was added OR the resulting
// tar file has become big enough, it will call the uploadAndDelete() method.
// To upload a lot of tarfiles, you should only have to create one TarCache.
// The TarCache takes care of creating each tarfile and getting it uploaded.
type TarCache struct {
	fileChannel    <-chan tarfile.LocalDataFile
	timeoutChannel chan string
	currentTarfile map[string]tarfile.Tarfile
	sizeThreshold  bytecount.ByteCount
	ageThreshold   time.Duration
	rootDirectory  string
	uploader       uploader.Uploader
}

// New creates a new TarCache object and returns a pointer to it and the
// channel used to send data to the TarCache.
func New(rootDirectory string, sizeThreshold bytecount.ByteCount, ageThreshold time.Duration, uploader uploader.Uploader) (*TarCache, chan<- tarfile.LocalDataFile) {
	if !strings.HasSuffix(rootDirectory, "/") {
		rootDirectory += "/"
	}
	// By giving the channel a large buffer, we attempt to decouple file
	// discovery event response times from any file processing times.
	fileChannel := make(chan tarfile.LocalDataFile, 1000000)
	tarCache := &TarCache{
		fileChannel:    fileChannel,
		timeoutChannel: make(chan string),
		rootDirectory:  rootDirectory,
		currentTarfile: make(map[string]tarfile.Tarfile),
		sizeThreshold:  sizeThreshold,
		ageThreshold:   ageThreshold,
		uploader:       uploader,
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
			pusherTarfilesUploadCalls.WithLabelValues("age_threshold_met").Inc()
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
func (t *TarCache) add(filename tarfile.LocalDataFile) {
	if warning := lintFilename(filename); warning != nil {
		log.Println("Strange filename encountered:", warning)
		pusherStrangeFilenames.Inc()
	}
	file, err := os.Open(string(filename))
	if err != nil {
		pusherFileOpenErrors.Inc()
		log.Printf("Could not open %s (error: %q)\n", filename, err)
		return
	}
	cleanedFilename := tarfile.LocalDataFile(strings.TrimPrefix(string(filename), t.rootDirectory))
	subdir := cleanedFilename.Subdir()
	if _, ok := t.currentTarfile[subdir]; !ok {
		t.currentTarfile[subdir] = tarfile.New(subdir)
	}
	tf := t.currentTarfile[subdir]
	tf.Add(cleanedFilename, file, t.makeTimer)
	if tf.Size() > t.sizeThreshold {
		pusherTarfilesUploadCalls.WithLabelValues("size_threshold_met").Inc()
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

// lintFilename returns nil if the file has a normal name, and an explanatory
// error about why the name is strange otherwise.
func lintFilename(ldf tarfile.LocalDataFile) error {
	name := string(ldf)
	cleaned := path.Clean(name)
	if cleaned != name {
		return fmt.Errorf("The cleaned up path %q did not match the name of the passed-in file %q", cleaned, name)
	}
	d, f := path.Split(name)
	if strings.HasPrefix(f, ".") {
		return fmt.Errorf("Hidden file detected: %q", name)
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("Too many dots in %v", name)
	}
	invalidChars := regexp.MustCompile(`[^a-zA-Z0-9/:._-]`)
	if invalidChars.MatchString(name) {
		return fmt.Errorf("Strange characters detected in the filename %q", name)
	}
	recommendedFormat := regexp.MustCompile(`^[a-zA-Z0-9_-]+/20[0-9][0-9]/[0-9]{2}/[0-9]{2}`)
	if !recommendedFormat.MatchString(d) {
		return fmt.Errorf("Directory structure does not mirror our best practices for file %v", name)
	}
	return nil
}
