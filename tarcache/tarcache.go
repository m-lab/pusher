// Package tarcache supports the creation and running of a pipeline that
// receives files, tars up the contents, and uploads everything when the tarfile
// is big enough or the contents are old enough.
package tarcache

import (
	"archive/tar"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/m-lab/pusher/uploader"

	"github.com/m-lab/go/bytecount"
	"github.com/m-lab/go/rtx"
)

var (
	pusherTarfilesCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_tarfiles_created_total",
		Help: "The number of tarfiles the pusher has created",
	})
	pusherTarfilesUploadCalls = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_tarfiles_upload_calls_total",
			Help: "The number of times upload has been called",
		},
		[]string{"reason"},
	)
	pusherTarfilesUploaded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_tarfiles_successful_uploads_total",
		Help: "The number of tarfiles the pusher has uploaded",
	})
	pusherFilesPerTarfile = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "pusher_files_per_tarfile",
		Help:    "The number of files in each tarfile the pusher has uploaded",
		Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000},
	})
	pusherBytesPerTarfile = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "pusher_bytes_per_tarfile",
		Help:    "The number of bytes in each tarfile the pusher has uploaded",
		Buckets: []float64{1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9},
	})
	pusherBytesPerFile = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "pusher_bytes_per_file",
		Help:    "The number of bytes in each file the pusher has uploaded",
		Buckets: []float64{1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9},
	})
	pusherTarfileDuplicateFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_tarfiles_duplicates_total",
		Help: "The number of times we attempted to add a file twice to the same tarfile",
	})
	pusherFileReadErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_file_read_errors_total",
		Help: "The number of times we could not read a file that we were trying to add to the tarfile",
	})
	pusherTarfilesUploadRetry = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_tarfile_upload_retries_total",
		Help: "The number of times we have had to retry uploading a file.",
	})
	pusherTarfilesUploadMaxRetry = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_tarfile_upload_max_retries_total",
		Help: "The number of times we have retried and hit our maximum retry backoff",
	})
	pusherFilesAdded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_files_added_total",
		Help: "The number of files we have added to a tarfile",
	})
	pusherFilesRemoved = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_files_removed_total",
		Help: "The number of files we have removed from the disk after upload",
	})
	pusherFileRemoveErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_file_remove_errors_total",
		Help: "The number of times the os.Remove call failed",
	})
	pusherEmptyUploads = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_empty_uploads_total",
		Help: "The number of times we tried to upload a tarfile with nothing in it",
	})
	pusherCurrentTarfileFileCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pusher_current_tarfile_files",
		Help: "The number of files in the current tarfile",
	})
	pusherCurrentTarfileSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pusher_current_tarfile_size_bytes",
		Help: "The number of bytes in the current tarfile",
	})
	pusherStrangeFilenames = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_strange_filenames_total",
		Help: "The number of files we have seen with names that looked surprising in some way",
	})
	pusherSuccessTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pusher_success_timestamp",
		Help: "The unix timestamp of the most recent pusher success",
	})
)

func init() {
	prometheus.MustRegister(pusherTarfilesCreated)
	prometheus.MustRegister(pusherTarfilesUploadCalls)
	prometheus.MustRegister(pusherTarfilesUploaded)
	prometheus.MustRegister(pusherFilesPerTarfile)
	prometheus.MustRegister(pusherBytesPerTarfile)
	prometheus.MustRegister(pusherBytesPerFile)
	prometheus.MustRegister(pusherTarfileDuplicateFiles)
	prometheus.MustRegister(pusherFileReadErrors)
	prometheus.MustRegister(pusherFilesAdded)
	prometheus.MustRegister(pusherFilesRemoved)
	prometheus.MustRegister(pusherFileRemoveErrors)
	prometheus.MustRegister(pusherEmptyUploads)
	prometheus.MustRegister(pusherCurrentTarfileFileCount)
	prometheus.MustRegister(pusherCurrentTarfileSize)
	prometheus.MustRegister(pusherStrangeFilenames)
	prometheus.MustRegister(pusherSuccessTimestamp)
}

// A LocalDataFile is the absolute pathname of a data file.
type LocalDataFile string

// Subdir returns the subdirectory of the LocalDataFile, up to 3 levels deep.
func (l LocalDataFile) Subdir() string {
	dirs := strings.Split(string(l), "/")
	if len(dirs) <= 1 {
		log.Printf("File handed to the tarcache is not in a subdirectory: %v is not split by /", l)
		return ""
	}
	k := len(dirs) - 1
	if k > 3 {
		k = 3
	}
	return strings.Join(dirs[:k], "/")
}

// TarCache contains everything you need to incrementally create a tarfile.
// Once enough time has passed since the first file was added OR the resulting
// tar file has become big enough, it will call the uploadAndDelete() method.
// To upload a lot of tarfiles, you should only have to create one TarCache.
// The TarCache takes care of creating each tarfile and getting it uploaded.
type TarCache struct {
	fileChannel    <-chan LocalDataFile
	timeoutChannel chan string
	currentTarfile map[string]*tarfile
	sizeThreshold  bytecount.ByteCount
	ageThreshold   time.Duration
	rootDirectory  string
	uploader       uploader.Uploader
}

// New creates a new TarCache object and returns a pointer to it and the
// channel used to send data to the TarCache.
func New(rootDirectory string, sizeThreshold bytecount.ByteCount, ageThreshold time.Duration, uploader uploader.Uploader) (*TarCache, chan<- LocalDataFile) {
	if !strings.HasSuffix(rootDirectory, "/") {
		rootDirectory += "/"
	}
	// By giving the channel a large buffer, we attempt to decouple file
	// discovery event response times from any file processing times.
	fileChannel := make(chan LocalDataFile, 1000000)
	tarCache := &TarCache{
		fileChannel:    fileChannel,
		timeoutChannel: make(chan string),
		rootDirectory:  rootDirectory,
		currentTarfile: make(map[string]*tarfile),
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

// Add adds the contents of a file to the underlying tarfile.  It possibly
// calls uploadAndDelete() afterwards.
func (t *TarCache) add(filename LocalDataFile) {
	if warning := lintFilename(filename); warning != nil {
		log.Println("Strange filename encountered:", warning)
		pusherStrangeFilenames.Inc()
	}
	subdir := filename.Subdir()
	if _, ok := t.currentTarfile[subdir]; !ok {
		t.currentTarfile[subdir] = newTarfile(subdir)
	}
	tf := t.currentTarfile[subdir]
	if _, present := tf.memberSet[filename]; present {
		pusherTarfileDuplicateFiles.Inc()
		log.Printf("Not adding %q to the tarfile a second time.\n", filename)
		return
	}
	contents, err := ioutil.ReadFile(string(filename))
	if err != nil {
		pusherFileReadErrors.Inc()
		log.Printf("Could not read %s (error: %q)\n", filename, err)
		return
	}
	pusherBytesPerFile.Observe(float64(len(contents)))
	header := &tar.Header{
		Name: strings.TrimPrefix(string(filename), t.rootDirectory),
		Mode: 0666,
		Size: int64(len(contents)),
	}

	// It's not at all clear how any of the below errors might be recovered from,
	// so we treat them as unrecoverable using Must, and hope that the errors
	// are transient and will not re-occur when the container is restarted.
	rtx.Must(tf.tarWriter.WriteHeader(header), "Could not write the tarfile header for %v", filename)
	_, err = tf.tarWriter.Write(contents)
	rtx.Must(err, "Could not write the tarfile contents for %v", filename)

	// Flush the data so that our in-memory filesize is accurate.
	rtx.Must(tf.tarWriter.Flush(), "Could not flush the tarWriter")
	rtx.Must(tf.gzipWriter.Flush(), "Could not flush the gzipWriter")

	if len(tf.members) == 0 {
		log.Println("Starting timer for " + subdir)
		tf.timeout = time.AfterFunc(t.ageThreshold, func() {
			t.timeoutChannel <- subdir
		})
	}
	pusherFilesAdded.Inc()
	tf.members = append(tf.members, filename)
	tf.memberSet[filename] = struct{}{}
	pusherCurrentTarfileFileCount.Set(float64(len(tf.members)))
	pusherCurrentTarfileSize.Set(float64(tf.contents.Len()))
	if bytecount.ByteCount(tf.contents.Len()) > t.sizeThreshold {
		t.uploadAndDelete(subdir)
		pusherTarfilesUploadCalls.WithLabelValues("size_threshold_met").Inc()
	}
}

// Upload the buffer, delete the component files, start a new buffer.
func (t *TarCache) uploadAndDelete(subdir string) {
	if tf, ok := t.currentTarfile[subdir]; ok {
		tf.uploadAndDelete(t.uploader)
		delete(t.currentTarfile, subdir)
	} else {
		log.Printf("Upload called for nonexistent tarfile for directory %q\n", subdir)
	}
}

// lintFilename returns nil if the file has a normal name, and an explanatory
// error about why the name is strange otherwise.
func lintFilename(ldf LocalDataFile) error {
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
