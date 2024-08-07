package tarfile

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/m-lab/go/bytecount"

	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/backoff"
	"github.com/m-lab/pusher/filename"
	"github.com/m-lab/pusher/uploader"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	addFile  = "add_file"
	skipFile = "skip_file"
)

var (
	pusherTarfilesCreated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_tarfiles_created_total",
			Help: "The number of tarfiles the pusher has created",
		},
		[]string{"datatype"})
	pusherTarfilesUploaded = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_tarfiles_successful_uploads_total",
			Help: "The number of tarfiles the pusher has uploaded",
		},
		[]string{"datatype"})
	pusherFilesPerTarfile = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pusher_files_per_tarfile",
			Help:    "The number of files in each tarfile the pusher has uploaded",
			Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000},
		},
		[]string{"datatype"})
	pusherBytesPerTarfile = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pusher_bytes_per_tarfile",
			Help:    "The number of bytes in each tarfile the pusher has uploaded",
			Buckets: []float64{1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9},
		},
		[]string{"datatype"})
	pusherBytesPerFile = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pusher_bytes_per_file",
			Help:    "The number of bytes in each file the pusher has uploaded",
			Buckets: []float64{1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9},
		},
		[]string{"datatype"})
	pusherTarfileDuplicateFiles = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_tarfiles_duplicates_total",
			Help: "The number of times we attempted to add a file twice to the same tarfile",
		},
		[]string{"datatype", "condition"})
	pusherFileReadErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_file_read_errors_total",
			Help: "The number of times we could not read or stat a file that we were trying to add to the tarfile",
		},
		[]string{"datatype"})
	pusherFilesAdded = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_files_added_total",
			Help: "The number of files we have added to a tarfile",
		},
		[]string{"datatype"})
	pusherFilesSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_files_skipped_total",
			Help: "The number of files we have skipped in the tarfile",
		},
		[]string{"datatype"})
	pusherFilesRemoved = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_files_removed_total",
			Help: "The number of files we have removed from the disk after upload",
		},
		[]string{"datatype", "condition"})
	pusherFileRemoveErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_file_remove_errors_total",
			Help: "The number of times the os.Remove call failed",
		},
		[]string{"datatype", "condition"})
	pusherEmptyUploads = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_empty_uploads_total",
			Help: "The number of times we tried to upload a tarfile with nothing in it",
		},
		[]string{"datatype"})
	pusherSuccessTimestamp = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pusher_success_timestamp",
			Help: "The unix timestamp of the most recent pusher success",
		},
		[]string{"datatype"})
)

// A tarfile represents a single tar file containing data for upload
type tarfile struct {
	timeout    *time.Timer
	members    map[filename.Internal]filename.System
	skipped    map[filename.Internal]filename.System
	contents   *bytes.Buffer
	tarWriter  *tar.Writer
	gzipWriter *gzip.Writer
	subdir     filename.System
	datatype   string
	fileRatio  float64
	metadata   map[string]string
}

// Tarfile represents all the capabilities of a tarfile.  You can add files to it, upload it, and check its size.
type Tarfile interface {
	Add(filename.Internal, osFile, func(string) *time.Timer)
	UploadAndDelete(uploader uploader.Uploader)
	Size() bytecount.ByteCount
	SkippedCount() int
}

// New creates a new tarfile to hold the contents of a particular subdirectory.
func New(subdir filename.System, datatype string, ratio float64, metadata map[string]string) Tarfile {
	pusherTarfilesCreated.WithLabelValues(datatype).Inc()
	// TODO: profile and determine if preallocation is a good idea.
	buffer := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	metadata["MLAB.datatype"] = datatype
	return &tarfile{
		contents:   buffer,
		tarWriter:  tarWriter,
		gzipWriter: gzipWriter,
		members:    make(map[filename.Internal]filename.System),
		skipped:    make(map[filename.Internal]filename.System),
		subdir:     subdir,
		datatype:   datatype,
		fileRatio:  ratio,
		metadata:   metadata,
	}
}

// osFile exists to allow fake files to be handed to the Add() method to allow
// the testing of error conditions. All os.File objects satisfy this interface.
type osFile interface {
	io.ReadCloser
	Stat() (os.FileInfo, error)
	Name() string
}

// Add adds a single file to the tarfile, and starts a timer if the file is the
// first file added.
func (t *tarfile) Add(cleanedFilename filename.Internal, file osFile, timerFactory func(string) *time.Timer) {
	// Check if file has already been skipped.
	if _, present := t.skipped[cleanedFilename]; present {
		pusherTarfileDuplicateFiles.WithLabelValues(t.datatype, skipFile).Inc()
		log.Printf("Not adding %q to the skipped files a second time.\n", cleanedFilename)
		return
	}

	// Check if file has already been added.
	if _, present := t.members[cleanedFilename]; present {
		pusherTarfileDuplicateFiles.WithLabelValues(t.datatype, addFile).Inc()
		log.Printf("Not adding %q to the tarfile a second time.\n", cleanedFilename)
		return
	}

	// Check if file should be skipped.
	if rand.Float64() >= t.fileRatio {
		t.skipped[cleanedFilename] = filename.System(file.Name())
		pusherFilesSkipped.WithLabelValues(t.datatype).Inc()
		return
	}

	// Add file.
	fstat, err := file.Stat()
	if err != nil {
		pusherFileReadErrors.WithLabelValues(t.datatype).Inc()
		log.Printf("Could not stat %s (error: %q)\n", cleanedFilename, err)
		return
	}
	size := fstat.Size()
	pusherBytesPerFile.WithLabelValues(t.datatype).Observe(float64(size))
	// We read the file into memory instead of using io.Copy directly into the
	// tarfile because if the use of io.Copy goes wrong, then we have to make
	// the error fatal (because the already-written tarfile headers are now
	// wrong), while the reading of disk into RAM, if it goes wrong, simply
	// causes us to ignore the file and return.
	//
	// As things stand we have to choose between mitigating the risk of too-large
	// files or mitigating the risk of unreadable files. The code below mitigates
	// the risk of unreadable files. If too-large files become a problem, delete
	// everything up to the declaration of the header and then replace the
	// `io.Copy(t.tarWriter, contents)` line with `io.Copy(tarWriter, file)`.
	contents := &bytes.Buffer{}
	_, err = io.Copy(contents, file)
	if err != nil {
		pusherFileReadErrors.WithLabelValues(t.datatype).Inc()
		log.Printf("Could not read %s (error: %q)\n", cleanedFilename, err)
		return
	}
	header := &tar.Header{
		Name:       string(cleanedFilename),
		Mode:       0666,
		Size:       size,
		ModTime:    fstat.ModTime(),
		PAXRecords: t.metadata,
	}

	// It's not at all clear how any of the below errors might be recovered from,
	// so we treat them as unrecoverable using Must, and hope that the errors
	// are transient and will not re-occur when the container is restarted.
	rtx.Must(t.tarWriter.WriteHeader(header), "Could not write the tarfile header for %v", cleanedFilename)
	_, err = io.Copy(t.tarWriter, contents)
	rtx.Must(err, "Could not write the tarfile contents for %v", cleanedFilename)

	// Flush the data so that our in-memory filesize is accurate.
	rtx.Must(t.tarWriter.Flush(), "Could not flush the tarWriter")
	rtx.Must(t.gzipWriter.Flush(), "Could not flush the gzipWriter")

	if len(t.members) == 0 {
		t.timeout = timerFactory(string(t.subdir))
	}
	pusherFilesAdded.WithLabelValues(t.datatype).Inc()
	t.members[cleanedFilename] = filename.System(file.Name())
}

// Upload the contents of the tarfile and then delete the component files. This
// function will never return unsuccessfully. If there are files to upload, this
// method will keep trying until the upload succeeds.
func (t *tarfile) UploadAndDelete(uploader uploader.Uploader) {
	// Delete skipped files.
	for _, filename := range t.skipped {
		t.removeFile(filename, skipFile)
	}

	if len(t.members) == 0 {
		pusherEmptyUploads.WithLabelValues(t.datatype).Inc()
		pusherSuccessTimestamp.WithLabelValues(t.datatype).SetToCurrentTime()
		log.Println("uploadAndDelete called on an empty tarfile.")
		return
	}
	if t.timeout != nil {
		t.timeout.Stop()
	}
	t.tarWriter.Close()
	t.gzipWriter.Close()
	pusherFilesPerTarfile.WithLabelValues(t.datatype).Observe(float64(len(t.members)))
	pusherBytesPerTarfile.WithLabelValues(t.datatype).Observe(float64(t.contents.Len()))
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
	pusherTarfilesUploaded.WithLabelValues(t.datatype).Inc()
	pusherSuccessTimestamp.WithLabelValues(t.datatype).SetToCurrentTime()
	for _, filename := range t.members {
		t.removeFile(filename, addFile)
	}
}

func (t tarfile) Size() bytecount.ByteCount {
	return bytecount.ByteCount(t.contents.Len())
}

// SkippedCount returns the number of files skipped in the tarfile given
// the datatype's file upload ratio.
func (t tarfile) SkippedCount() int {
	return len(t.skipped)
}

func (t tarfile) removeFile(filename filename.System, condition string) {
	// If the file can't be removed, then it either was already removed or the
	// remove call failed for some unknown reason (permissions, maybe?). If the
	// file still exists after this attempted remove, then it should eventually
	// get picked up by the finder.
	if err := os.Remove(string(filename)); err == nil {
		pusherFilesRemoved.WithLabelValues(t.datatype, condition).Inc()
	} else {
		pusherFileRemoveErrors.WithLabelValues(t.datatype, condition).Inc()
		log.Printf("Failed to remove %s file %v (error: %q)\n", condition, filename, err)
	}
}
