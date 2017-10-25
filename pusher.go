package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"flag"
	//"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	//"sort"
	"time"
)


var (
	directory             = flag.String("directory", "/var/spool/ndt_iupui", "The directory containing the files to upload.")
	upload_server         = flag.String("upload_server", "https://uploader-mlab-oti.appspot.com", "What server to use to get the upload URL")
	data_buffer_threshold = flag.Int("data_buffer_threshold", 30000000, "The number of bytes to buffer before uploading")
	min_file_age          = flag.Duration("min_file_age", time.Duration(2)*time.Hour, "The amount of time that must have elapsed since the last edit of a file must be before it is eligible for upload")
)


type LocalDataFile struct {
	full_relative_name string
	info os.FileInfo
}


func findFiles() (error, []*LocalDataFile) {
	eligible_files := make([]*LocalDataFile, 0)
	eligible_time := time.Now().Add(-(*min_file_age))
	total_eligible_size := int64(0)

	err := filepath.Walk(*directory, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if eligible_time.After(info.ModTime()) {
			local_data_file := &LocalDataFile {
				full_relative_name: path,
				info: info,
			}
			eligible_files = append(eligible_files, local_data_file)
			total_eligible_size += info.Size()
		}
		return nil
	})

        if err != nil {
		log.Printf("Could not walk %s (err=%s)", *directory, err)
		return err, eligible_files
	}
	log.Printf("Total file sizes = %d", total_eligible_size)
        if total_eligible_size > int64(*data_buffer_threshold) {
		return nil, eligible_files
	} else {
		return nil, make([]*LocalDataFile, 0)
	}
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


func findUploadAndDelete() error {
        err, files := findFiles()
	if err != nil {
		return err
	}
	err, tarfile_contents := createTarfile(files)
	log.Printf("%s", tarfile_contents)
	// Upload?
	// Delete?
	return nil
}


func main() {
	flag.Parse()
	findUploadAndDelete()
}
