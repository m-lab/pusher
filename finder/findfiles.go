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

// FindFiles recursively searches through a given directory to find all the files which are old enough to be eligible for upload.
// The list of files returned is sorted by mtime.
func FindFiles(directory string, minFileAge time.Duration) ([]*tarcache.LocalDataFile, error) {
	// Give an initial capacity to the slice. 1024 chosen because it's a nice round number.
	// TODO: Choose a better default.
	eligibleFiles := make([]*tarcache.LocalDataFile, 0, 1024)
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
			localDataFile := &tarcache.LocalDataFile{
				AbsoluteFileName: path,
				Info:             info,
			}
			eligibleFiles = append(eligibleFiles, localDataFile)
			totalEligibleSize += info.Size()
		}
		return nil
	})

	if err != nil {
		log.Printf("Could not walk %s (err=%s)", directory, err)
		return eligibleFiles, err
	}

	pusherFinderRuns.Inc()
	pusherFinderFiles.Add(float64(len(eligibleFiles)))
	pusherFinderBytes.Add(float64(totalEligibleSize))

	// Sort the files by mtime
	sort.Slice(eligibleFiles, func(i, j int) bool {
		return eligibleFiles[i].Info.ModTime().Before(eligibleFiles[j].Info.ModTime())
	})
	return eligibleFiles, nil
}

// FindForever repeatedly runs FindFiles.
//
// It randomizes the inter-`find` sleep time in an effort to avoid thundering
// herd problems after container restarts. We're not worried about overloading
// GCS, but without this we might end up running `find` for every experiment
// simultaneously forever, and this could periodically run the disk out of
// IOPs. We use ExpFloat64 to ensure that the inter-`find` time is the
// exponential distribution and that the time-distribution of `find` operations
// is therefore memoryless.
func FindForever(directory string, maxFileAge time.Duration, notificationChannel chan<- *tarcache.LocalDataFile, expectedSleepTime time.Duration) {
	for {
		time.Sleep(time.Duration(rand.ExpFloat64()*expectedSleepTime.Seconds()) * time.Second)

		files, err := FindFiles(directory, maxFileAge)
		if err != nil {
			log.Printf("Could not FindFiles: %v", err)
			continue
		}

		for _, file := range files {
			notificationChannel <- file
		}
	}
}
