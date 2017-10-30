package pusher

// Thise function has been placed in its own file because, in the fullness of
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
func FindFiles(directory string, min_file_age time.Duration) (error, []*LocalDataFile) {
	eligible_files := make([]*LocalDataFile, 0)
	eligible_time := time.Now().Add(-min_file_age)
	total_eligible_size := int64(0)

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if eligible_time.After(info.ModTime()) {
			local_data_file := &LocalDataFile {
				full_relative_name: path,
				info: info,
				cached_size: info.Size(),
			}
			eligible_files = append(eligible_files, local_data_file)
			total_eligible_size += info.Size()
		}
		return nil
	})

        if err != nil {
		log.Printf("Could not walk %s (err=%s)", directory, err)
		return err, eligible_files
	}
	log.Printf("Total file sizes = %d", total_eligible_size)
	log.Printf("Total file count = %d", len(eligible_files))
	// Sort the files by mtime
	sort.Slice(eligible_files, func(i, j int) bool {
		return eligible_files[i].info.ModTime().Before(eligible_files[j].info.ModTime())
	})
	return nil, eligible_files
}


