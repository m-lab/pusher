package tarfile

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
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

// InternalFilename is the pathname of a data file inside of the tarfile.
type InternalFilename string

// Subdir returns the subdirectory of the LocalDataFile, up to 3 levels deep. It
// is only guaranteed to work right on relative path names, suitable for
// inclusion in tarfiles.
func (l InternalFilename) Subdir() string {
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

// Lint returns nil if the file has a normal name, and an explanatory error
// about why the name is strange otherwise.
func (l InternalFilename) Lint() error {
	name := string(l)
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

// A tarfile represents a single tar file containing data for upload
type tarfile struct {
	timeout    *time.Timer
	members    []osFile
	memberSet  map[InternalFilename]struct{}
	contents   *bytes.Buffer
	tarWriter  *tar.Writer
	gzipWriter *gzip.Writer
	subdir     string
}

// Tarfile represents all the capabilities of a tarfile.  You can add files to it, upload it, and check its size.
type Tarfile interface {
	Add(InternalFilename, osFile, func(string) *time.Timer)
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
		memberSet:  make(map[InternalFilename]struct{}),
		subdir:     dir,
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
func (t *tarfile) Add(cleanedFilename InternalFilename, file osFile, timerFactory func(string) *time.Timer) {
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
		pusherFileReadErrors.Inc()
		log.Printf("Could not read %s (error: %q)\n", cleanedFilename, err)
		return
	}
	if n, err := file.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		pusherFileReadErrors.Inc()
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
		t.timeout = timerFactory(t.subdir)
	}
	pusherFilesAdded.Inc()
	t.members = append(t.members, file)
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
		if err := os.Remove(file.Name()); err == nil {
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
