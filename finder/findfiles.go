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

	"github.com/m-lab/pusher/fileinfo"
)

// FindFiles recursively searches through a given directory to find all the files which are old enough to be eligible for upload.
// The list of files returned is sorted by mtime.
func FindFiles(directory string, minFileAge time.Duration) ([]*fileinfo.LocalDataFile, error) {
	eligibleFiles := make([]*fileinfo.LocalDataFile, 0)
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
			localDataFile := &fileinfo.LocalDataFile{
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
	log.Printf("Total file sizes = %d", totalEligibleSize)
	log.Printf("Total file count = %d", len(eligibleFiles))
	// Sort the files by mtime
	sort.Slice(eligibleFiles, func(i, j int) bool {
		return eligibleFiles[i].Info.ModTime().Before(eligibleFiles[j].Info.ModTime())
	})
	return eligibleFiles, nil
}
