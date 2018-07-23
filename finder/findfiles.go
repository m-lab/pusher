// Package finder provides a `find`-like interface to file discovery. In the
// fullness of time, we expect Pusher to use inotify, and so this more
// IO-intensive approach will not be needed and this code will be deleted.
package finder

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/m-lab/pusher/tarcache"
)

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
	// TODO: add metrics
	log.Printf("Total file sizes = %d", totalEligibleSize)
	log.Printf("Total file count = %d", len(eligibleFiles))
	// Sort the files by mtime
	sort.Slice(eligibleFiles, func(i, j int) bool {
		return eligibleFiles[i].Info.ModTime().Before(eligibleFiles[j].Info.ModTime())
	})
	return eligibleFiles, nil
}
