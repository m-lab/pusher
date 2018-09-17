package tarcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/go/bytecount"
	r "github.com/m-lab/go/runtimeext"
)

type fakeUploader struct {
	contents         []byte
	calls            int
	requestedRetries int
}

func (f *fakeUploader) Upload(contents []byte) error {
	f.contents = contents
	f.calls++
	if f.requestedRetries > 0 {
		f.requestedRetries--
		return errors.New("A fake error to trigger retry logic")
	}
	return nil
}

type FileInTarfile struct {
	name string
	size int
}

// verifyTarfileContents checks that the referenced tarfile actually contains
// each file in contents.  The filenames should not contain characters which
// have a special meaning in a regular expression context.
func verifyTarfileContents(t *testing.T, tarfile string, contents []FileInTarfile) {
	// Get the table of files in the tarfile.
	cmd := exec.Command("tar", "tvfz", tarfile)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		t.Fatalf("tar command failed: %q", err)
	}
	// All files are unseen initially.
	seenFile := make([]bool, len(contents))
	// For each line in the table of output, check it against each file.
	for _, lineString := range strings.Split(string(out.Bytes()), "\n") {
		if lineString == "" {
			continue
		}
		line := []byte(lineString)
		matched := false
		for i, f := range contents {
			re := fmt.Sprintf(" %d .* %s$", f.size, f.name)
			if match, err := regexp.Match(re, line); match && err == nil {
				matched = true
				seenFile[i] = true
			}
		}
		// Every line should match some file, or else there are random
		// extra files present.
		if !matched {
			t.Errorf("Bad line: %q", line)
		}
	}
	// If any file is unseen, report an error.
	for i, seen := range seenFile {
		if !seen {
			t.Errorf("Did not find file %s in the output of tar", contents[i].name)
		}
	}
}

// A whitebox test that verifies that the cache contents are built up gradually.
func TestAdd(t *testing.T) {
	tempdir, err := ioutil.TempDir("/tmp", "tarcache.TestAdd")
	defer os.RemoveAll(tempdir)
	if err != nil {
		t.Error(err)
		return
	}

	// Make the data files, one small and one big.
	ioutil.WriteFile(tempdir+"/tinyfile", []byte("abcdefgh"), os.FileMode(0666))
	bigcontents := make([]byte, 2000)
	rand.Read(bigcontents)
	os.MkdirAll(tempdir+"/a/b", 0700)
	ioutil.WriteFile(tempdir+"/a/b/bigfile", bigcontents, os.FileMode(0666))

	uploader := fakeUploader{
		requestedRetries: 1,
	}
	// Ignore the returned channel - this is a whitebox test.
	tarCache, _ := New(tempdir, bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(1*time.Hour), &uploader)
	if tarCache.currentTarfile.contents.Len() != 0 {
		t.Errorf("The file should be of zero length and is not (%d != 0)", tarCache.currentTarfile.contents.Len())
	}
	// Add the tiny file, which should not trigger an upload.
	fileObject, err := os.Open(tempdir + "/tinyfile")
	r.Must(err, "Open failed")
	fileStat, err := fileObject.Stat()
	r.Must(err, "Stat failed")
	tinyFile := LocalDataFile{
		AbsoluteFileName: tempdir + "/tinyfile",
		Info:             fileStat,
	}
	tarCache.add(&tinyFile)
	// Add the tiny file a second time, which should not do anything at all.
	tarCache.add(&tinyFile)
	if tarCache.currentTarfile.contents.Len() == 0 {
		t.Errorf("The file should be of nonzero length and is not (%d == 0)", tarCache.currentTarfile.contents.Len())
	}
	if len(tarCache.currentTarfile.members) != 1 {
		t.Errorf("The tarCache should have just one  member and it has %d", len(tarCache.currentTarfile.members))
	}
	if uploader.calls != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Add the big file, which should trigger an upload and file deletion.
	fileObject, err = os.Open(tempdir + "/a/b/bigfile")
	r.Must(err, "Open failed")
	fileStat, err = fileObject.Stat()
	r.Must(err, "Stat failed")
	bigFile := LocalDataFile{
		AbsoluteFileName: tempdir + "/a/b/bigfile",
		Info:             fileStat,
	}
	tarCache.add(&bigFile)
	if uploader.calls == 0 {
		t.Error("uploader.calls should be >0 ")
	}
	if contents, err := ioutil.ReadFile(tempdir + "/tinyfile"); err == nil {
		t.Errorf("tinyfile was not deleted, but should have been - contents are %q", string(contents))
	}
	// Ensure that the uploaded tarfile can be opened by tar and contains files of the correct size.
	ioutil.WriteFile(tempdir+"/tarfile.tgz", uploader.contents, os.FileMode(0666))
	verifyTarfileContents(t, tempdir+"/tarfile.tgz",
		[]FileInTarfile{
			{name: "tinyfile", size: 8},
			{name: "a/b/bigfile", size: 2000}})
	// Now add one more file to make sure that the cache still works after upload.
	ioutil.WriteFile(tempdir+"/tiny2", []byte("12345678"), os.FileMode(0666))
	fileObject, err = os.Open(tempdir + "/tiny2")
	r.Must(err, "Open failed")
	fileStat, err = fileObject.Stat()
	r.Must(err, "Stat failed")
	tiny2File := LocalDataFile{
		AbsoluteFileName: tempdir + "/tiny2",
		Info:             fileStat,
	}
	if len(tarCache.currentTarfile.members) != 0 || tarCache.currentTarfile.contents.Len() != 0 {
		t.Error("Failed to clear the cache after upload")
	}
	tarCache.add(&tiny2File)
	if len(tarCache.currentTarfile.members) != 1 || tarCache.currentTarfile.contents.Len() == 0 {
		t.Error("Failed to add the new file after upload")
	}
}

func TestTimer(t *testing.T) {
	tempdir, err := ioutil.TempDir("/tmp", "tarcache.TestTimer")
	defer os.RemoveAll(tempdir)
	if err != nil {
		t.Error(err)
		return
	}

	// Make a small data file.
	ioutil.WriteFile(tempdir+"/tinyfile", []byte("abcdefgh"), os.FileMode(0666))
	bigcontents := make([]byte, 2000)
	rand.Read(bigcontents)
	os.MkdirAll(tempdir+"/a/b", 0700)
	ioutil.WriteFile(tempdir+"/a/b/bigfile", bigcontents, os.FileMode(0666))

	uploader := fakeUploader{}
	tarCache, channel := New(tempdir, bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(100*time.Millisecond), &uploader)
	// Add the small file, which should not trigger an upload.
	fileObject, err := os.Open(tempdir + "/tinyfile")
	r.Must(err, "Open failed")
	fileStat, err := fileObject.Stat()
	r.Must(err, "Stat failed")
	tinyFile := LocalDataFile{
		AbsoluteFileName: tempdir + "/tinyfile",
		Info:             fileStat,
	}
	ctx := context.Background()
	go tarCache.ListenForever(ctx)
	channel <- &tinyFile
	if uploader.calls != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Sleep to cause a timeout.
	time.Sleep(time.Duration(250 * time.Millisecond))
	// Verify that the timer fired.
	if uploader.calls == 0 {
		t.Error("uploader.calls should be nonzero ", uploader.calls)
	}

	// Do it again to verify that the timer setup doesn't break after a single use.
	uploader.calls = 0
	// With no files added, the timer should not fire.
	time.Sleep(time.Duration(250 * time.Millisecond))
	if uploader.calls != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Create a tiny file and add it.
	ioutil.WriteFile(tempdir+"/tiny2", []byte("12345678"), os.FileMode(0666))
	fileObject, err = os.Open(tempdir + "/tiny2")
	r.Must(err, "Open failed")
	fileStat, err = fileObject.Stat()
	r.Must(err, "Stat failed")
	tiny2File := LocalDataFile{
		AbsoluteFileName: tempdir + "/tiny2",
		Info:             fileStat,
	}
	channel <- &tiny2File
	if uploader.calls != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Wait for the timer to fire.
	time.Sleep(time.Duration(250 * time.Millisecond))
	if uploader.calls != 1 {
		t.Error("uploader.calls should be one ", uploader.calls)
	}
}

func TestContextCancellation(t *testing.T) {
	uploader := fakeUploader{}
	tarCache, _ := New("/tmp", bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(100*time.Millisecond), &uploader)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	// If this doesn't actually listen forever, then this test is a success.
	tarCache.ListenForever(ctx)
}

func TestChannelCloseCancellation(t *testing.T) {
	uploader := fakeUploader{}
	tarCache, inputChannel := New("/tmp", bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(100*time.Millisecond), &uploader)
	ctx := context.Background()
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(inputChannel)
	}()
	// If this doesn't actually listen forever, then this test is a success.
	tarCache.ListenForever(ctx)
}

func TestEmptyUpload(t *testing.T) {
	tempdir, err := ioutil.TempDir("/tmp", "tarcache.TestEmptyUpload")
	defer os.RemoveAll(tempdir)
	if err != nil {
		t.Error(err)
		return
	}
	uploader := fakeUploader{}
	// Ignore the returned channel - this is a whitebox test.
	tarCache, _ := New(tempdir, bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(1*time.Hour), &uploader)
	tarCache.uploadAndDelete()
	if uploader.calls != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}

	ioutil.WriteFile(tempdir+"/tinyfile", []byte("abcdefgh"), os.FileMode(0666))
	// Add the small file, which should not trigger an upload.
	fileObject, err := os.Open(tempdir + "/tinyfile")
	r.Must(err, "Open failed")
	fileStat, err := fileObject.Stat()
	r.Must(err, "Stat failed")
	tinyFile := LocalDataFile{
		AbsoluteFileName: tempdir + "/tinyfile",
		Info:             fileStat,
	}
	tarCache.add(&tinyFile)

	if err = os.Remove(tempdir + "/tinyfile"); err != nil {
		t.Errorf("Could not remove the tinyfile: %v", err)
	}

	// This should not crash, even though we removed the tinyfile out from underneath the uploader.
	tarCache.uploadAndDelete()
}

func TestUnreadableFile(t *testing.T) {
	tempdir, err := ioutil.TempDir("/tmp", "tarcache.TestUnreadableFile")
	defer os.RemoveAll(tempdir)
	if err != nil {
		t.Error(err)
		return
	}
	uploader := fakeUploader{}
	// Ignore the returned channel - this is a whitebox test.
	tarCache, _ := New(tempdir, bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(1*time.Hour), &uploader)
	ioutil.WriteFile(tempdir+"/tinyfile", []byte("abcdefgh"), os.FileMode(0666))
	// Add the small file, which should not trigger an upload.
	fileObject, err := os.Open(tempdir + "/tinyfile")
	r.Must(err, "Open failed")
	fileStat, err := fileObject.Stat()
	r.Must(err, "Stat failed")
	tinyFile := LocalDataFile{
		AbsoluteFileName: tempdir + "/tinyfile",
		Info:             fileStat,
	}
	if err = os.Remove(tempdir + "/tinyfile"); err != nil {
		t.Errorf("Could not remove the tinyfile: %v", err)
	}
	// This should not crash, even though we removed the tinyfile out from underneath the uploader.
	tarCache.add(&tinyFile)
	if len(tarCache.currentTarfile.members) != 0 {
		t.Error("We added a nonexistent file to the tarCache.")
	}
}
