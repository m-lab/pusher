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
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/m-lab/pusher/tarcache"
)

// Set up the prometheus metrics.
var (
	pusherFinderRuns = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_finder_runs_total",
		Help: "How many times has FindFiles been called",
	})
	pusherFinderFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_finder_files_found_total",
		Help: "How many files has FindFiles found",
	})
	pusherFinderBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pusher_finder_bytes_found_total",
		Help: "How many bytes has FindFiles found",
	})
)

func init() {
	prometheus.MustRegister(pusherFinderRuns)
	prometheus.MustRegister(pusherFinderFiles)
	prometheus.MustRegister(pusherFinderBytes)
}

// findFiles recursively searches through a given directory to find all the files which are old enough to be eligible for upload.
// The list of files returned is sorted by mtime.
func findFiles(directory string, minFileAge time.Duration) []tarcache.SystemFilename {
	// Give an initial capacity to the slice. 1024 chosen because it's a nice round number.
	// TODO: Choose a better default.
	eligibleFiles := make(map[tarcache.SystemFilename]os.FileInfo)
	eligibleTime := time.Now().Add(-minFileAge)
	totalEligibleSize := int64(0)

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// Any error terminates the walk.
			return err
		}
		if info.IsDir() {
			return nil
		}
		if eligibleTime.After(info.ModTime()) {
			eligibleFiles[tarcache.SystemFilename(path)] = info
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
	fileList := make([]tarcache.SystemFilename, 0, len(eligibleFiles))
	for f := range eligibleFiles {
		fileList = append(fileList, f)
	}
	sort.Slice(fileList, func(i, j int) bool {
		iInfo := eligibleFiles[fileList[i]]
		jInfo := eligibleFiles[fileList[j]]
		return iInfo.ModTime().Before(jInfo.ModTime())
	})
	return fileList
}

// FindForever repeatedly runs FindFiles until its context is canceled.
//
// It randomizes the inter-`find` sleep time in an effort to avoid thundering
// herd problems after container restarts. We're not worried about overloading
// GCS, but without this we might end up running `find` for every experiment
// simultaneously forever, and this could periodically run the disk out of
// IOPs. We use ExpFloat64 to ensure that the inter-`find` time is the
// exponential distribution and that the time-distribution of `find` operations
// is therefore memoryless.
func FindForever(ctx context.Context, directory string, maxFileAge time.Duration, notificationChannel chan<- tarcache.SystemFilename, expectedSleepTime time.Duration) {
	for {
		sleepTime := time.Duration(rand.ExpFloat64()*float64(expectedSleepTime.Nanoseconds())) * time.Nanosecond
		select {
		case <-ctx.Done():
			return
		case <-time.NewTimer(sleepTime).C:
		}

		files := findFiles(directory, maxFileAge)
		for _, file := range files {
			notificationChannel <- file
		}
	}
}
