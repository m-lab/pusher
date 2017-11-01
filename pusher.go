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

// LocalDataFile are just FileInfo objects that also keep track of the filename
// and the cached value of the file size.  We keep track of the filename
// because FileInfo just has the basename, and we want to preserve directory
// structure inside our tarfiles, and we cache the size because we keep track
// of the running total size of all files in the tarfile, and if a file gets
// written to after we already buffered it, then that would potentially throw
// off our math. So we need to know not just what size the file is, but also
// what size we previously thought the file was.
type LocalDataFile struct {
	FullRelativeName string
	Info             os.FileInfo
	// Cache the size so sums of filesize are always using the same
	// integer value, in case the file grows in size between walk and
	// upload.
	CachedSize int64
}

// StartUploader starts a goroutine that accepts pointers to LocalDataFile
// objects and will periodically tar, upload, and delete the full set of files
// that have been uploaded.  This function, the returned channel, and the
// LocalDataFile struct constitute the complete public interface of pusher.
// The filename of any uploaded tar file will be based on the min mtime of the
// files it contains.  To prevent the possibility of name collisions, send
// files down the channel in ascending order by mtime.  To terminate the
// Uploader, send a nil down the channel as a signal final value which will
// cause any buffered data to be sent to the server and then the channel to be
// closed.
func StartUploader(server string, dataBufferThreshold int64) chan *LocalDataFile {
	deleteChannel := make(chan *string)
	go removeAll(deleteChannel)

	uploadChannel := make(chan *uploadSpec)
	go upload(server, uploadChannel, deleteChannel)

	tarChannel := make(chan *tarContents)
	go tarUp(tarChannel, uploadChannel)

	bufferChannel := make(chan *LocalDataFile)
	go bufferFiles(dataBufferThreshold, bufferChannel, tarChannel)
	return bufferChannel
}

// From here on down is the guts of the module, which should not be relevant to
// the public interface.

type tarContents struct {
	files    *[]*LocalDataFile
	minMtime time.Time
}

type uploadSpec struct {
	contents *[]byte
	files    *[]*LocalDataFile
	minMtime time.Time
}

// bufferThenProcess receives files on the inputChannel, and then, when the
// time is right (the condition is described exactly in the function, but
// roughly corresponds to "every day but maybe sooner but not slicing on
// sub-second boundaries") the files buffered are sent all at once down the
// outputChannel.
func bufferFiles(dataBufferThreshold int64, inputChannel chan *LocalDataFile, outputChannel chan *tarContents) {
	const Day = time.Duration(24) * time.Hour
	filesDiscovered := make(map[string]*LocalDataFile)
	// Try to upload all remaining data upon function termination.
	defer func() {
		processFileMap(filesDiscovered, outputChannel)
		close(outputChannel)
	}()

	totalSize := int64(0)
	var earliestMtime, previousMtime time.Time
	for file := range inputChannel {
		if file == nil {
			log.Printf("Received a nil LocalDataFile: exiting")
			return
		}
		fileMtime := file.Info.ModTime()
		if earliestMtime.IsZero() {
			earliestMtime = fileMtime
		}
		olderFile, exists := filesDiscovered[file.FullRelativeName]
		if exists {
			totalSize -= olderFile.CachedSize
			delete(filesDiscovered, file.FullRelativeName)
		}
		// If we have some new files, and the new file has a mtime
		// ahead of our previous files, and either the collected
		// quantity of file data is big enough, or we have crossed a
		// day boundary, then we should upload.
		if len(filesDiscovered) > 0 &&
			!previousMtime.Equal(fileMtime.Truncate(time.Second)) &&
			(totalSize+file.CachedSize > dataBufferThreshold || !earliestMtime.Truncate(Day).Equal(fileMtime.Truncate(Day))) {
			processFileMap(filesDiscovered, outputChannel)
			filesDiscovered = make(map[string]*LocalDataFile)
			earliestMtime = time.Time{}
			totalSize = int64(0)
		}
		// Add the discovered file to the set of files.
		filesDiscovered[file.FullRelativeName] = file
		totalSize += file.CachedSize
		if earliestMtime.IsZero() || file.Info.ModTime().Before(earliestMtime) {
			earliestMtime = file.Info.ModTime()
		}
		previousMtime = fileMtime.Truncate(time.Second)
	}
}

func processFileMap(filesDiscovered map[string]*LocalDataFile, outputChannel chan *tarContents) {
	if len(filesDiscovered) == 0 {
		return
	}
	var minMtime time.Time
	files := make([]*LocalDataFile, len(filesDiscovered))
	for _, localFile := range filesDiscovered {
		files = append(files, localFile)
		if minMtime.IsZero() || localFile.Info.ModTime().Before(minMtime) {
			minMtime = localFile.Info.ModTime()
		}
	}
	outputChannel <- &tarContents{
		files:    &files,
		minMtime: minMtime,
	}
}

// TODO: Actually upload files
func upload(server string, inputChannel chan *uploadSpec, outputChannel chan *string) {
	for fileToUpload := range inputChannel {
		log.Printf("Faking uploading using %s with the time %s and %d bytes", fileToUpload.minMtime, server, len(*fileToUpload.contents))
		for _, f := range *(fileToUpload.files) {
			outputChannel <- &f.FullRelativeName
		}
	}
	close(outputChannel)
}

// TODO: Actually remove files
func removeAll(inputChannel chan *string) {
	for filename := range inputChannel {
		log.Printf("Faking the deletion of %s", filename)
	}
}

func tarUp(inputChannel chan *tarContents, outputChannel chan *uploadSpec) {
	for tarFileSpec := range inputChannel {
		contents, err := createTarfile(*tarFileSpec.files)
		if err == nil {
			byteArray := contents.Bytes()
			outputChannel <- &uploadSpec{
				files:    tarFileSpec.files,
				contents: &byteArray,
				minMtime: tarFileSpec.minMtime,
			}
		} else {
			log.Printf("Could not create tarfile: %s", err)
		}
	}
	close(outputChannel)
}

func createTarfile(files []*LocalDataFile) (*bytes.Buffer, error) {
	buffer := new(bytes.Buffer)
	gzipWriter := gzip.NewWriter(buffer)
	tarfileWriter := tar.NewWriter(gzipWriter)
	// Close the Writers down in the right order upon exit.
	defer func() {
		tarfileWriter.Close()
		gzipWriter.Close()
	}()

	for _, file := range files {
		header, err := tar.FileInfoHeader(file.Info, file.FullRelativeName)
		if err != nil {
			log.Printf("Error creating header: %s", err)
			return nil, err
		}
		if err := tarfileWriter.WriteHeader(header); err != nil {
			log.Printf("Error writing header: %s", err)
			return nil, err
		}
		contents, err := ioutil.ReadFile(file.FullRelativeName)
		if err != nil {
			log.Printf("Error reading file: %s", err)
			return nil, err
		}
		if _, err := tarfileWriter.Write(contents); err != nil {
			log.Printf("Error writing tarfile: %s", err)
			return nil, err
		}
	}
	return buffer, nil
}
