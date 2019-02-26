package tarfile

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"log"
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
		[]string{"datatype"})
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
	pusherFilesRemoved = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_files_removed_total",
			Help: "The number of files we have removed from the disk after upload",
		},
		[]string{"datatype"})
	pusherFileRemoveErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_file_remove_errors_total",
			Help: "The number of times the os.Remove call failed",
		},
		[]string{"datatype"})
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
	members    []osFile
	memberSet  map[filename.Internal]struct{}
	contents   *bytes.Buffer
	tarWriter  *tar.Writer
	gzipWriter *gzip.Writer
	subdir     filename.System
	datatype   string
}

// Tarfile represents all the capabilities of a tarfile.  You can add files to it, upload it, and check its size.
type Tarfile interface {
	Add(filename.Internal, osFile, func(string) *time.Timer)
	UploadAndDelete(uploader uploader.Uploader)
	Size() bytecount.ByteCount
}

// New creates a new tarfile to hold the contents of a particular subdirectory.
func New(subdir filename.System, datatype string) Tarfile {
	pusherTarfilesCreated.WithLabelValues(datatype).Inc()
	// TODO: profile and determine if preallocation is a good idea.
	buffer := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	return &tarfile{
		contents:   buffer,
		tarWriter:  tarWriter,
		gzipWriter: gzipWriter,
		memberSet:  make(map[filename.Internal]struct{}),
		subdir:     subdir,
		datatype:   datatype,
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
	if _, present := t.memberSet[cleanedFilename]; present {
		pusherTarfileDuplicateFiles.WithLabelValues(t.datatype).Inc()
		log.Printf("Not adding %q to the tarfile a second time.\n", cleanedFilename)
		return
	}
	fstat, err := file.Stat()
	if err != nil {
		pusherFileReadErrors.WithLabelValues(t.datatype).Inc()
		log.Printf("Could not stat %s (error: %q)\n", cleanedFilename, err)
		return
	}
	size := fstat.Size()
	pusherBytesPerFile.WithLabelValues(t.datatype).Observe(float64(size))
	// We read the file into memory instead of using io.Copy because if the use of
	// io.Copy goes wrong, then we have to make the error fatal (because the
	// already-written tarfile headers are now wrong), while the reading of disk
	// into RAM, if it goes wrong, simply causes us to ignore the file and return.
	//
	// As things stand we have to choose between mitigating the risk of too-large
	// files or mitigating the risk of unreadable files. The code below mitigates
	// the risk of unreadable files. If too-large files become a problem, delete
	// everything up to the declaration of the header and then replace the
	// `tarWriter.Write(contents)` line with `io.Copy(tarWriter, file)`.
	contents := make([]byte, size)
	if n, err := file.Read(contents); int64(n) != size || err != nil {
		pusherFileReadErrors.WithLabelValues(t.datatype).Inc()
		log.Printf("Could not read %s (error: %q)\n", cleanedFilename, err)
		return
	}
	if n, err := file.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		pusherFileReadErrors.WithLabelValues(t.datatype).Inc()
		log.Printf("Could not after reading %d bytes, %s was not at EOF (error: %q)\n", size, cleanedFilename, err)
		return
	}
	header := &tar.Header{
		Name: string(cleanedFilename),
		Mode: 0666,
		Size: size,
	}

	// It's not at all clear how any of the below errors might be recovered from,
	// so we treat them as unrecoverable using Must, and hope that the errors
	// are transient and will not re-occur when the container is restarted.
	rtx.Must(t.tarWriter.WriteHeader(header), "Could not write the tarfile header for %v", cleanedFilename)
	_, err = t.tarWriter.Write(contents)
	rtx.Must(err, "Could not write the tarfile contents for %v", cleanedFilename)

	// Flush the data so that our in-memory filesize is accurate.
	rtx.Must(t.tarWriter.Flush(), "Could not flush the tarWriter")
	rtx.Must(t.gzipWriter.Flush(), "Could not flush the gzipWriter")

	if len(t.members) == 0 {
		t.timeout = timerFactory(string(t.subdir))
	}
	pusherFilesAdded.WithLabelValues(t.datatype).Inc()
	t.members = append(t.members, file)
	t.memberSet[cleanedFilename] = struct{}{}
}

// Upload the contents of the tarfile and then delete the component files. This
// function will never return unsuccessfully. If there are files to upload, this
// method will keep trying until the upload succeeds.
func (t *tarfile) UploadAndDelete(uploader uploader.Uploader) {
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
	for _, file := range t.members {
		// If the file can't be removed, then it either was already removed or the
		// remove call failed for some unknown reason (permissions, maybe?). If the
		// file still exists after this attempted remove, then it should eventually
		// get picked up by the finder.
		if err := os.Remove(file.Name()); err == nil {
			pusherFilesRemoved.WithLabelValues(t.datatype).Inc()
		} else {
			pusherFileRemoveErrors.WithLabelValues(t.datatype).Inc()
			log.Printf("Failed to remove %v (error: %q)\n", file, err)
		}
	}
}

func (t tarfile) Size() bytecount.ByteCount {
	return bytecount.ByteCount(t.contents.Len())
}
