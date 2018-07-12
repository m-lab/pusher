package tarcache

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/pusher/bytecount"
	"github.com/m-lab/pusher/fileinfo"
)

type fakeUploader struct {
	contents         []byte
	calls            int
	requestedRetries int
}

func (f *fakeUploader) Upload(contents *bytes.Buffer) error {
	f.contents = contents.Bytes()
	f.calls++
	if f.requestedRetries > 0 {
		f.requestedRetries--
		return errors.New("A fake error to trigger retry logic")
	}
	return nil
}

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
	tarCache, _ := New(tempdir, bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(1*time.Hour), &uploader)
	if tarCache.currentTarfile.contents.Len() != 0 {
		t.Errorf("The file should be of zero length and is not (%d != 0)", tarCache.currentTarfile.contents.Len())
	}
	// Add the tiny file, which should not trigger an upload.
	fileObject, _ := os.Open(tempdir + "/tinyfile")
	fileStat, _ := fileObject.Stat()
	tinyFile := fileinfo.LocalDataFile{
		AbsoluteFileName: tempdir + "/tinyfile",
		Info:             fileStat,
	}
	tarCache.add(&tinyFile)
	if tarCache.currentTarfile.contents.Len() == 0 {
		t.Errorf("The file should be of nonzero length and is not (%d == 0)", tarCache.currentTarfile.contents.Len())
	}
	if len(tarCache.currentTarfile.members) != 1 {
		t.Errorf("The tarCache should have a member and it does not")
	}
	if uploader.calls != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Add the big file, which should trigger an upload and file deletion.
	fileObject, _ = os.Open(tempdir + "/a/b/bigfile")
	fileStat, _ = fileObject.Stat()
	bigFile := fileinfo.LocalDataFile{
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
	cmd := exec.Command("tar", "tvfz", tempdir+"/tarfile.tgz")
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		t.Errorf("tar command failed: %q", err)
	}
	hasTinyfile := false
	hasBigfile := false
	for _, lineString := range strings.Split(string(out.Bytes()), "\n") {
		line := []byte(lineString)
		if match, err := regexp.Match(` 8 .* tinyfile$`, line); match && err == nil {
			hasTinyfile = true
		} else if match, err := regexp.Match(` 2000 .* a/b/bigfile$`, line); match && err == nil {
			hasBigfile = true
		} else if lineString != "" {
			t.Errorf("Bad line: %q", line)
		}
	}
	if !hasBigfile || !hasTinyfile {
		t.Errorf("Both should be true, but hasBigfile = %t and hasTinyfile = %t", hasBigfile, hasTinyfile)
	}
	// Now add one more file to make sure that the cache still works after upload.
	ioutil.WriteFile(tempdir+"/tiny2", []byte("12345678"), os.FileMode(0666))
	fileObject, _ = os.Open(tempdir + "/tiny2")
	fileStat, _ = fileObject.Stat()
	tiny2File := fileinfo.LocalDataFile{
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
	fileObject, _ := os.Open(tempdir + "/tinyfile")
	fileStat, _ := fileObject.Stat()
	tinyFile := fileinfo.LocalDataFile{
		AbsoluteFileName: tempdir + "/tinyfile",
		Info:             fileStat,
	}
	go tarCache.ListenForever()
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
	fileObject, _ = os.Open(tempdir + "/tiny2")
	fileStat, _ = fileObject.Stat()
	tiny2File := fileinfo.LocalDataFile{
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
