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


func StartUploader(server string, dataBufferThreshold int64) chan *LocalDataFile {
	channel := make(chan *LocalDataFile)
	go BufferTarUploadAndDelete(server, dataBufferThreshold, channel)
	return channel
}


// Exported for test purposes only
func BufferTarUploadAndDelete(server string, dataBufferThreshold int64, channel chan *LocalDataFile) {
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
		}
		filesDiscovered[file.FullRelativeName] = file
		totalSize += file.CachedSize

		fYear, fMonth, fDay := file.Info.ModTime().Date()
		eYear, eMonth, eDay := earliestMtime.Date()
		if totalSize > dataBufferThreshold || !(eYear == fYear && eMonth == fMonth && eDay == fDay) {
			var minMtime time.Time
			files := make([]*LocalDataFile, len(filesDiscovered))
			for , localFile := range filesDiscovered {
				files = append(files, localFile)
				if minMtime.IsZero() || file.Info.ModTime().Before(minMtime) {
					minMtime = file.Info.ModTime()
				}
			}
			TarUploadAndDelete(files, minMtime, server)
			filesDiscovered.Clear()
			earliestMtime = time.Zero
			totalSize = int64(0)
		}
	}
}


func Upload(buff *bytes.Buffer, minMtime time.Time, server string) error {
	log.Printf("Faking uploading %d files using %s with the time %s and %d bytes", len(files), minMtime, server, buff.Len())
	return nil
}


func Delete(files []*LocalDataFile) error {
	for _, file := range files {
		log.Printf("Faking the deletion of %s", files.FullRelativeName)
	}
	return nil
}


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


func CreateTarfile(files []*LocalDataFile) (error, *bytes.Buffer) {
	buffer := new(bytes.Buffer)
        gzipWriter := gzip.NewWriter(buffer)
	tarfileWriter := tar.NewWriter(gzipWriter)
	defer tarfileWriter.Close()
	for , file := range files {
		header, err := tar.FileInfoHeader(file.Info, file.FullRelativeName)
		if err != nil {
			return err, nil
		}
		if err := tarfileWriter.WriteHeader(header); err != nil {
			return err, nil
		}
		contents, err := ioutil.ReadFile(file.FullRelativeName)
		if err != nil {
			return err, nil
		}
		if , err := tarfileWriter.Write(contents); err != nil {
			return err, nil
		}
	}
	return nil, buffer
}
