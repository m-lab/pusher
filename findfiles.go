package pusher

// This function has been placed in its own file because, in the fullness of
// time, we expect Pusher to become a library used by the inotify exporter, and
// for this function to therefore become unused.

import (
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// FindFiles recursively searches through a given directory to find all the files which are old enough to be eligible for upload.
// The list of files returned is sorted by mtime.
func FindFiles(directory string, minFileAge time.Duration) (error, []*LocalDataFile) {
	eligibleFiles := make([]*LocalDataFile, 0)
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
			localDataFile := &LocalDataFile {
				FullRelativeName: path,
				Info: info,
				CachedSize: info.Size(),
			}
			eligibleFiles = append(eligibleFiles, localDataFile)
			totalEligibleSize += info.Size()
		}
		return nil
	})

        if err != nil {
		log.Printf("Could not walk %s (err=%s)", directory, err)
		return err, eligibleFiles
	}
	log.Printf("Total file sizes = %d", totalEligibleSize)
	log.Printf("Total file count = %d", len(eligibleFiles))
	// Sort the files by mtime
	sort.Slice(eligibleFiles, func(i, j int) bool {
		return eligibleFiles[i].Info.ModTime().Before(eligibleFiles[j].Info.ModTime())
	})
	return nil, eligibleFiles
}


