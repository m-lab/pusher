// Package finder provides a `find`-like interface to file discovery.
//
// Even with inotify, we need a find-based cleanup for two reasons:
//
// 1. If closed files exist in the directory when the program starts, there is
// no way to know whether they are open or not. So, if they are older than the
// max_file_age, we will assume that the files are closed and upload them.
//
// 2. There is a race condition in the notify library where it is possible to
// create a directory and then create a file in the directory before the
// recursive listener has been established. We work around this bug (and any
// other bugs) by having a "cleanup" job that unconditionally adds any files
// older than the max_file_age.
package finder

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/m-lab/go/memoryless"
	"github.com/m-lab/pusher/filename"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Set up the prometheus metrics.
var (
	pusherFinderRuns = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pusher_finder_runs_total",
		Help: "How many times has FindFiles been called",
	})
	pusherFinderFiles = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pusher_finder_files_found_total",
		Help: "How many files has FindFiles found",
	})
	pusherFinderBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pusher_finder_bytes_found_total",
		Help: "How many bytes has FindFiles found",
	})
	pusherFinderMtimeLowerBound = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pusher_finder_mtime_lower_bound",
			Help: "Timestamp of the oldest file discovered by the finder",
		},
		[]string{"datatype"},
	)
)

// findFiles recursively searches through a given directory to find all the files which are old enough to be eligible for upload.
// The list of files returned is sorted by mtime.
func findFiles(datatype string, directory filename.System, minFileAge time.Duration) []filename.System {
	// Give an initial capacity to the slice. 1024 chosen because it's a nice round number.
	// TODO: Choose a better default.
	eligibleFiles := make(map[filename.System]os.FileInfo)
	eligibleTime := time.Now().Add(-minFileAge)
	totalEligibleSize := int64(0)

	err := filepath.Walk(string(directory), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Any error terminates the walk.
			return err
		}
		if info.IsDir() {
			return nil
		}
		if eligibleTime.After(info.ModTime()) {
			eligibleFiles[filename.System(path)] = info
			totalEligibleSize += info.Size()
		}
		return nil
	})

	if err != nil {
		log.Printf("Could not walk %s (err=%s). Proceeding with any discovered files.", directory, err)
	}

	pusherFinderRuns.Inc()
	pusherFinderFiles.Add(float64(len(eligibleFiles)))
	pusherFinderBytes.Add(float64(totalEligibleSize))

	// Sort the files by mtime
	fileList := make([]filename.System, 0, len(eligibleFiles))
	for f := range eligibleFiles {
		fileList = append(fileList, f)
	}
	sort.Slice(fileList, func(i, j int) bool {
		iInfo := eligibleFiles[fileList[i]]
		jInfo := eligibleFiles[fileList[j]]
		return iInfo.ModTime().Before(jInfo.ModTime())
	})
	if len(fileList) > 0 {
		pusherFinderMtimeLowerBound.WithLabelValues(datatype).Set(float64(eligibleFiles[fileList[0]].ModTime().Unix()))
	} else {
		pusherFinderMtimeLowerBound.WithLabelValues(datatype).SetToCurrentTime()
	}
	return fileList
}

// FindForever repeatedly runs FindFiles until its context is canceled.
//
// It randomizes the inter-`find` sleep time in an effort to avoid thundering
// herd problems after container restarts. We're not worried about overloading
// GCS, but without this we might end up running `find` for every experiment
// simultaneously forever, and this could periodically run the disk out of
// IOPs. We use the memoryless library to ensure that the inter-`find` time is
// the exponential distribution and that the time-distribution of `find`
// operations is therefore memoryless.
func FindForever(ctx context.Context, datatype string, directory filename.System, maxFileAge time.Duration, notificationChannel chan<- filename.System, times memoryless.Config) {
	memoryless.Run(
		ctx,
		func() {
			files := findFiles(datatype, directory, maxFileAge)
			for _, file := range files {
				notificationChannel <- file
			}
		},
		times)
}
