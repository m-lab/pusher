package pusher

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"log"
	"os"
	"time"
)


type LocalDataFile struct {
	full_relative_name string
	info os.FileInfo
	cached_size int64
}


func StartUploader(server string, data_buffer_threshold int64) chan *LocalDataFile {
	channel := make(chan *LocalDataFile)
	go bufferTarAndUpload(server, data_buffer_threshold, channel)
	return channel
}


func bufferTarAndUpload(server string, data_buffer_threshold int64, channel chan *LocalDataFile) {
	files_discovered := make(map[string]*LocalDataFile)
	total_size := int64(0)
	var earliest_mtime time.Time
	for {
		file := <-channel
		if earliest_mtime.IsZero()  {
			earliest_mtime = file.info.ModTime()
		}
		older_file, exists := files_discovered[file.full_relative_name]
		if exists {
			total_size -= older_file.cached_size
		}
		files_discovered[file.full_relative_name] = file
		total_size += file.cached_size

		f_year, f_month, f_day := file.info.ModTime().Date()
		e_year, e_month, e_day := earliest_mtime.Date()
		if total_size > data_buffer_threshold || !(e_year == f_year && e_month == f_month && e_day == f_day) {
			var min_mtime time.Time
			files := make([]*LocalDataFile, len(files_discovered))
			for _, local_file := range files_discovered {
				files = append(files, local_file)
				if min_mtime.IsZero() || file.info.ModTime().Before(min_mtime) {
					min_mtime = file.info.ModTime()
				}
			}
			tarAndUpload(files, min_mtime, server)
		}
	}
}


func tarAndUpload(files []*LocalDataFile, min_mtime time.Time, server string) error {
	err, buff := createTarfile(files)
	log.Printf("Faking uploading %d files using %s with the time %s and %d bytes", len(files), min_mtime, server, buff.Len())
	return err
}


func createTarfile(files []*LocalDataFile) (error, *bytes.Buffer) {
	buffer := new(bytes.Buffer)
        gzip_writer := gzip.NewWriter(buffer)
	tarfile_writer := tar.NewWriter(gzip_writer)
	defer tarfile_writer.Close()
	for _, file := range files {
		header, err := tar.FileInfoHeader(file.info, file.full_relative_name)
		if err != nil {
			return err, nil
		}
		if err := tarfile_writer.WriteHeader(header); err != nil {
			return err, nil
		}
		contents, err := ioutil.ReadFile(file.full_relative_name)
		if err != nil {
			return err, nil
		}
		if _, err := tarfile_writer.Write(contents); err != nil {
			return err, nil
		}
	}
	return nil, buffer
}
