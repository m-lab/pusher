package tarfile

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/m-lab/go/bytecount"

	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/backoff"
	"github.com/m-lab/pusher/uploader"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	pusherTarfilesCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_tarfiles_created_total",
		Help: "The number of tarfiles the pusher has created",
	})
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
		Help: "The number of times we could not read or stat a file that we were trying to add to the tarfile",
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
	pusherCurrentTarfileFilesCreated = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pusher_current_tarfile_files",
		Help: "The number of files in the current tarfile",
	})
	pusherTotalTarfileSize = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_tarfile_size_bytes_total",
		Help: "The number of bytes we've ever put in a tarfile",
	})
	pusherSuccessTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pusher_success_timestamp",
		Help: "The unix timestamp of the most recent pusher success",
	})
)

func init() {
	prometheus.MustRegister(pusherTarfilesCreated)
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
	prometheus.MustRegister(pusherCurrentTarfileFilesCreated)
	prometheus.MustRegister(pusherTotalTarfileSize)
	prometheus.MustRegister(pusherSuccessTimestamp)
}

// A LocalDataFile is the pathname of a data file.
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

// Tarfile represents all the capabilities of a tarfile.  You can add files to it, upload it, and check its size.
type Tarfile interface {
	Add(LocalDataFile, *os.File, func(string) *time.Timer)
	UploadAndDelete(uploader uploader.Uploader)
	Size() bytecount.ByteCount
}

// New creates a new tarfile to hold the contents of a particular subdirectory.
func New(dir string) Tarfile {
	pusherTarfilesCreated.Inc()
	// TODO: profile and determine if preallocation is a good idea.
	buffer := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	return &tarfile{
		contents:   buffer,
		tarWriter:  tarWriter,
		gzipWriter: gzipWriter,
		memberSet:  make(map[LocalDataFile]struct{}),
		subdir:     dir,
	}
}

func (t *tarfile) Add(cleanedFilename LocalDataFile, file *os.File, timerFactory func(string) *time.Timer) {
	if _, present := t.memberSet[cleanedFilename]; present {
		pusherTarfileDuplicateFiles.Inc()
		log.Printf("Not adding %q to the tarfile a second time.\n", cleanedFilename)
		return
	}
	fstat, err := file.Stat()
	if err != nil {
		pusherFileReadErrors.Inc()
		log.Printf("Could not stat %s (error: %q)\n", cleanedFilename, err)
		return
	}
	size := fstat.Size()
	pusherBytesPerFile.Observe(float64(size))
	header := &tar.Header{
		Name: string(cleanedFilename),
		Mode: 0666,
		Size: size,
	}

	// It's not at all clear how any of the below errors might be recovered from,
	// so we treat them as unrecoverable using Must, and hope that the errors
	// are transient and will not re-occur when the container is restarted.
	rtx.Must(t.tarWriter.WriteHeader(header), "Could not write the tarfile header for %v", cleanedFilename)
	written, err := io.Copy(t.tarWriter, file)
	rtx.Must(err, "Could not write the tarfile contents for %v", cleanedFilename)
	if written != size {
		log.Fatalf("Wrote %d bytes for file %q instead of its length of %d bytes.", written, cleanedFilename, size)
	}

	// Flush the data so that our in-memory filesize is accurate.
	rtx.Must(t.tarWriter.Flush(), "Could not flush the tarWriter")
	rtx.Must(t.gzipWriter.Flush(), "Could not flush the gzipWriter")

	if len(t.members) == 0 {
		t.timeout = timerFactory(t.subdir)
	}
	pusherFilesAdded.Inc()
	t.members = append(t.members, cleanedFilename)
	t.memberSet[cleanedFilename] = struct{}{}
}

// Upload the contents of the tarfile and then delete the component files. This
// function will never return unsuccessfully. If there are files to upload, this
// method will keep trying until the upload succeeds.
func (t *tarfile) UploadAndDelete(uploader uploader.Uploader) {
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

func (t tarfile) Size() bytecount.ByteCount {
	return bytecount.ByteCount(t.contents.Len())
}
