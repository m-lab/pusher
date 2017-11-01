package pusher

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func TestUpload(t *testing.T) {
	t.Skip("Uploading is not yet implemented")
}

func TestRemoveAll(t *testing.T) {
	t.Skip("Deleting is not yet implemented")
}

// Make a file and return a LocalDataFile pointer for it.
func makeFile(filename string, contents string, stamp time.Time, t *testing.T) *LocalDataFile {
	err := ioutil.WriteFile(filename, []byte(contents), 0666)
	if err != nil {
		t.Error("Could not write %s: %s", filename, err)
	}
	os.Chtimes(filename, stamp, stamp)
	file, err := os.Open(filename)
	if err != nil {
		t.Error("Could not open %s: %s", filename, err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Error("Could not stat %s: %s", filename, err)
	}
	return &LocalDataFile{
		FullRelativeName: filename,
		Info:             info,
		CachedSize:       int64(len(contents)),
	}
}

func createAndEnterTempdir(t *testing.T) (string, func()) {
	tempdir, err := ioutil.TempDir("/tmp", "create_tarfile_test")
	if err != nil {
		t.Errorf("Bad tempdir: %s", err)
	}
	currdir, err := os.Getwd()
	if err != nil {
		t.Errorf("Bad Getcwd: %s", err)
	}
	err = os.Chdir(tempdir)
	if err != nil {
		t.Errorf("Bad Chdir: %s", err)
	}
	log.Printf("directory is: %s", tempdir)
	return tempdir, func() {
		os.Chdir(currdir)
		os.RemoveAll(tempdir)
	}
}

func TestCreateTarfile(t *testing.T) {
	_, cleanup := createAndEnterTempdir(t)
	defer cleanup()

	// Make a scratch area on the filesystem and make sure it gets cleaned up.
	// Add two files.
	fileList := []*LocalDataFile{
		makeFile("file1.txt", "twelve bytes", time.Time{}, t),
		makeFile("file2.txt", "fourteen bytes", time.Time{}, t),
	}
	// Call the function under test to make an in-memory tgz file.
	buffer, err := createTarfile(fileList)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	// Verify that the bytes received specify a valid tgz file with the right filenames in it.
	ioutil.WriteFile("files.tgz", buffer.Bytes(), 0666)
	out, err := exec.Command("tar", "tfz", "files.tgz").Output()
	if err != nil {
		t.Errorf("Bad command: %s", err)
	}
	if string(out) != "file1.txt\nfile2.txt\n" {
		t.Errorf("Bad output: %s", out)
	}
}

func TestBufferThenProcessWithLotsOfData(t *testing.T) {
	_, cleanup := createAndEnterTempdir(t)
	defer cleanup()
	filesSent := make([]*tarContents, 0)
	outputChannel := make(chan *tarContents)
	var fileWait, completeWait sync.WaitGroup
	processOutput := func() {
		for tc := range outputChannel {
			filesSent = append(filesSent, tc)
			fileWait.Done()
		}
		completeWait.Done()
	}
	go processOutput()
	inputChannel := make(chan *LocalDataFile)
	go bufferFiles(25, inputChannel, outputChannel)
	// Wait for one file to come down the pipe.
	fileWait.Add(1)
	stamp := time.Now().Add(time.Duration(-10) * time.Second)
	inputChannel <- makeFile("file1.txt", "twelve bytes", stamp, t)
	inputChannel <- makeFile("file2.txt", "twelve bytes", stamp.Add(time.Second), t)
	inputChannel <- makeFile("file3.txt", "twelve bytes", stamp.Add(time.Duration(2)*time.Second), t)
	fileWait.Wait()
	if len(filesSent) != 1 {
		t.Error("Wrong number of tarfile bundles received (%d)", len(filesSent))
	}
	fileWait.Add(1)
	completeWait.Add(1)
	close(inputChannel)
	fileWait.Wait()
	completeWait.Wait()
	if len(filesSent) != 2 {
		t.Error("Wrong number of tarfile bundles received after close (%d)", len(filesSent))
	}
}

func TestBufferThenProcessWhenTheDateChanges(t *testing.T) {
	_, cleanup := createAndEnterTempdir(t)
	defer cleanup()
	filesSent := make([]*tarContents, 0)
	outputChannel := make(chan *tarContents)
	var fileWait, completeWait sync.WaitGroup
	processOutput := func() {
		for tc := range outputChannel {
			filesSent = append(filesSent, tc)
			fileWait.Done()
		}
		completeWait.Done()
	}
	go processOutput()
	inputChannel := make(chan *LocalDataFile)
	go bufferFiles(25, inputChannel, outputChannel)
	fileWait.Add(1)
	oneday := time.Date(2016, 1, 28, 9, 45, 0, 0, time.UTC)
	nextday := time.Date(2016, 1, 29, 9, 45, 0, 0, time.UTC)
	// A twelve byte file should not cause a file to be sent down the pipe.
	inputChannel <- makeFile("file1.txt", "twelve bytes", oneday, t)
	// But then a second twelve byte file with the next day's timestamp should cause a push.
	inputChannel <- makeFile("file2.txt", "twelve bytes", nextday, t)
	fileWait.Wait()
	if len(filesSent) != 1 {
		t.Error("Wrong number of tarfile bundles received (%d)", len(filesSent))
	}
	fileWait.Add(1)
	completeWait.Add(1)
	close(inputChannel)
	fileWait.Wait()
	completeWait.Wait()
	if len(filesSent) != 2 {
		t.Error("Wrong number of tarfile bundles received after close (%d)", len(filesSent))
	}
}

func TestBufferThenProcessWontBreakSubsecond(t *testing.T) {
	_, cleanup := createAndEnterTempdir(t)
	defer cleanup()
	filesSent := make([]*tarContents, 0)
	outputChannel := make(chan *tarContents)
	var fileWait, completeWait sync.WaitGroup
	processOutput := func() {
		for tc := range outputChannel {
			filesSent = append(filesSent, tc)
			fileWait.Done()
		}
		completeWait.Done()
	}
	go processOutput()
	inputChannel := make(chan *LocalDataFile)
	go bufferFiles(10, inputChannel, outputChannel)
	// According to the size threshold, every 12 byte file should cause an
	// upload, but they are all the same timestamp, so we need to keep them
	// together.
	fileWait.Add(1)
	stamp := time.Now().Add(time.Duration(-10) * time.Second)
	inputChannel <- makeFile("file1.txt", "twelve bytes", stamp, t)
	inputChannel <- makeFile("file2.txt", "twelve bytes", stamp, t)
	inputChannel <- makeFile("file3.txt", "twelve bytes", stamp, t)
	inputChannel <- makeFile("file4.txt", "twelve bytes", stamp, t)
	inputChannel <- makeFile("file5.txt", "twelve bytes", stamp.Add(time.Second), t)
	fileWait.Wait()
	if len(filesSent) != 1 {
		t.Error("Wrong number of tarfile bundles received (%d)", len(filesSent))
	}
	fileWait.Add(1)
	completeWait.Add(1)
	close(inputChannel)
	fileWait.Wait()
	completeWait.Wait()
	if len(filesSent) != 2 {
		t.Error("Wrong number of tarfile bundles received after close (%d)", len(filesSent))
	}
}
