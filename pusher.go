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
	FullRelativeName string
	Info os.FileInfo
	// Cache the size so sums of filesize are always using the same
	// integer value, in case the file grows in size between walk and
	// upload.
	CachedSize int64
}


// StartUploader starts a goroutine that accepts pointers to LocalDataFile objects and will periodically tar, upload, and delete the full set of files that have been uploaded.
func StartUploader(server string, dataBufferThreshold int64) chan *LocalDataFile {
	channel := make(chan *LocalDataFile)
	go BufferThenCall(server, dataBufferThreshold, channel, TarUploadAndDelete)
	return channel
}


type FileProcessor func([]*LocalDataFile, time.Time, string) error


// Exported for test purposes only
func BufferThenCall(server string, dataBufferThreshold int64, channel chan *LocalDataFile, callback FileProcessor) {
	filesDiscovered := make(map[string]*LocalDataFile)
	totalSize := int64(0)
	var earliestMtime time.Time
	for {
		file := <-channel
		if earliestMtime.IsZero()  {
			earliestMtime = file.Info.ModTime()
		}
		olderFile, exists := filesDiscovered[file.FullRelativeName]
		if exists {
			totalSize -= olderFile.CachedSize
			delete(filesDiscovered, file.FullRelativeName)
		}
		fYear, fMonth, fDay := file.Info.ModTime().Date()
		eYear, eMonth, eDay := earliestMtime.Date()
		if len(filesDiscovered) > 0 && (totalSize + file.CachedSize > dataBufferThreshold ||
		                                !(eYear == fYear && eMonth == fMonth && eDay == fDay)) {
			var minMtime time.Time
			files := make([]*LocalDataFile, len(filesDiscovered))
			for _, localFile := range filesDiscovered {
				files = append(files, localFile)
				if minMtime.IsZero() || file.Info.ModTime().Before(minMtime) {
					minMtime = file.Info.ModTime()
				}
			}
			err := callback(files, minMtime, server)
			if err == nil {
				filesDiscovered = make(map[string]*LocalDataFile)
				earliestMtime = time.Time{}
				totalSize = int64(0)
			} else {
				log.Printf("Could not process %d files: %s", len(files), err)
			}
		}
		filesDiscovered[file.FullRelativeName] = file
		totalSize += file.CachedSize
		if earliestMtime.IsZero() || file.Info.ModTime().Before(earliestMtime) {
			earliestMtime = file.Info.ModTime()
		}
	}
}


// Exported for test purposes only
func Upload(buff *bytes.Buffer, minMtime time.Time, server string) error {
	log.Printf("Faking uploading using %s with the time %s and %d bytes", minMtime, server, buff.Len())
	return nil
}


// Exported for test purposes only
func Delete(files []*LocalDataFile) error {
	for _, file := range files {
		log.Printf("Faking the deletion of %s", file.FullRelativeName)
	}
	return nil
}


// Exported for test purposes only
func TarUploadAndDelete(files []*LocalDataFile, minMtime time.Time, server string) error {
	err, buff := CreateTarfile(files)
	if err != nil {
		return err
	}
	if err = Upload(buff, minMtime, server); err != nil {
		return err
	}
	return Delete(files)
}


// Exported for test purposes only
func CreateTarfile(files []*LocalDataFile) (error, *bytes.Buffer) {
	buffer := new(bytes.Buffer)
        gzipWriter := gzip.NewWriter(buffer)
	defer gzipWriter.Close()
	tarfileWriter := tar.NewWriter(gzipWriter)
	defer tarfileWriter.Close()

	for _, file := range files {
		header, err := tar.FileInfoHeader(file.Info, file.FullRelativeName)
		if err != nil {
			log.Printf("Error creating header: %s", err)
			return err, nil
		}
		if err := tarfileWriter.WriteHeader(header); err != nil {
			log.Printf("Error writing header: %s", err)
			return err, nil
		}
		contents, err := ioutil.ReadFile(file.FullRelativeName)
		if err != nil {
			log.Printf("Error reading file: %s", err)
			return err, nil
		}
		if _, err := tarfileWriter.Write(contents); err != nil {
			log.Printf("Error writing tarfile: %s", err)
			return err, nil
		}
	}
	return nil, buffer
}
