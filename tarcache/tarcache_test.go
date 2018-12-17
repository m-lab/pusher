package tarcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/go/bytecount"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/tarfile"
)

type fakeUploader struct {
	contents         []byte
	calls            int
	requestedRetries int
	expectedDir      string
}

func (f *fakeUploader) Upload(dir string, contents []byte) error {
	if f.expectedDir != "" && dir != f.expectedDir {
		log.Fatalf("Upload to unexpected directory: %v != %v\n", dir, f.expectedDir)
	}
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
	oldDir, err := os.Getwd()
	rtx.Must(err, "Could not get working directory")
	rtx.Must(os.Chdir(tempdir), "Could not chdir to the tempdir")
	defer os.Chdir(oldDir)

	// Make the data files, one small and one big.
	os.MkdirAll("a/b", 0700)
	ioutil.WriteFile("a/b/tinyfile", []byte("abcdefgh"), os.FileMode(0666))
	bigcontents := make([]byte, 2000)
	rand.Read(bigcontents)
	ioutil.WriteFile("a/b/bigfile", bigcontents, os.FileMode(0666))

	uploader := fakeUploader{
		requestedRetries: 1,
		expectedDir:      "a/b",
	}
	// Ignore the returned channel - this is a whitebox test.
	tarCache, _ := New(tempdir, bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(1*time.Hour), &uploader)
	if len(tarCache.currentTarfile) != 0 {
		t.Errorf("The file list should be of zero length and is not (%d != 0)", len(tarCache.currentTarfile))
	}
	// Add the tiny file, which should not trigger an upload.
	tinyFile := SystemFilename(tempdir + "/a/b/tinyfile")
	tarCache.add(tinyFile)
	// Add the tiny file a second time, which should not do anything at all.
	tarCache.add(tinyFile)
	if len(tarCache.currentTarfile) == 0 {
		t.Errorf("The file should be of nonzero length and is not (%d == 0)", len(tarCache.currentTarfile))
	}
	if uploader.calls != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Add the big file, which should trigger an upload and file deletion.
	bigFile := SystemFilename(tempdir + "/a/b/bigfile")
	tarCache.add(bigFile)
	if uploader.calls == 0 {
		t.Error("uploader.calls should be >0 ")
	}
	if contents, err := ioutil.ReadFile(tempdir + "/a/b/tinyfile"); err == nil {
		t.Errorf("tinyfile was not deleted, but should have been - contents are %q", string(contents))
	}
	// Ensure that the uploaded tarfile can be opened by tar and contains files of the correct size.
	ioutil.WriteFile("tarfile.tgz", uploader.contents, os.FileMode(0666))
	verifyTarfileContents(t, "tarfile.tgz",
		[]FileInTarfile{
			{name: "a/b/tinyfile", size: 8},
			{name: "a/b/bigfile", size: 2000}})
	// Now add one more file to make sure that the cache still works after upload.
	ioutil.WriteFile(tempdir+"/tiny2", []byte("12345678"), os.FileMode(0666))
	tiny2File := SystemFilename(tempdir + "/tiny2")
	if len(tarCache.currentTarfile) != 0 {
		t.Errorf("Failed to clear the cache after upload (%v)", len(tarCache.currentTarfile))
		for k := range tarCache.currentTarfile {
			t.Errorf("%q should not be in the cache", k)
		}
	}
	tarCache.add(tiny2File)
	if len(tarCache.currentTarfile) != 1 {
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
	oldDir, err := os.Getwd()
	rtx.Must(err, "Could not get working directory")
	rtx.Must(os.Chdir(tempdir), "Could not chdir to the tempdir")
	defer os.Chdir(oldDir)

	// Make a small data file.
	bigcontents := make([]byte, 2000)
	rand.Read(bigcontents)
	os.MkdirAll("a/b", 0700)
	os.MkdirAll("c/d", 0700)
	ioutil.WriteFile("a/b/tinyfile", []byte("abcdefgh"), os.FileMode(0666))
	ioutil.WriteFile("c/d/tinyfile", []byte("abcdefgh"), os.FileMode(0666))

	uploader := &fakeUploader{}
	tarCache, channel := New(tempdir, bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(100*time.Millisecond), uploader)
	// Add the small file, which should not trigger an upload.
	tinyFile := SystemFilename("a/b/tinyfile")
	otherTinyFile := SystemFilename("c/d/tinyfile")
	ctx := context.Background()
	go tarCache.ListenForever(ctx)
	channel <- tinyFile
	channel <- otherTinyFile
	if uploader.calls != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Sleep to cause a timeout.
	time.Sleep(250 * time.Millisecond)
	// Verify that the timer fired twice - once for each subdirectory.
	if uploader.calls != 2 {
		t.Error("uploader.calls should be 2 ", uploader.calls)
	}

	// Do it again to verify that the timer setup doesn't break after a single use.
	uploader.calls = 0
	// With no files added, the timer should not fire.
	time.Sleep(time.Duration(250 * time.Millisecond))
	if uploader.calls != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Create a tiny file and add it.
	ioutil.WriteFile("tiny2", []byte("12345678"), os.FileMode(0666))
	tiny2File := SystemFilename("tiny2")
	channel <- tiny2File
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
	uploader := fakeUploader{expectedDir: tempdir}
	// Ignore the returned channel - this is a whitebox test.
	tarCache, _ := New(tempdir, bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(1*time.Hour), &uploader)
	tarCache.currentTarfile[tempdir] = tarfile.New(tempdir)
	tarCache.uploadAndDelete("this does not exist")
	tarCache.uploadAndDelete(tempdir)
	if uploader.calls != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}

	ioutil.WriteFile(tempdir+"/tinyfile", []byte("abcdefgh"), os.FileMode(0666))
	// Add the small file, which should not trigger an upload.
	tarCache.add(SystemFilename(tempdir + "/tinyfile"))

	if err = os.Remove(tempdir + "/tinyfile"); err != nil {
		t.Errorf("Could not remove the tinyfile: %v", err)
	}

	// This should not crash, even though we removed the tinyfile out from underneath the uploader.
	tarCache.uploadAndDelete(tempdir)
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
	// This should not crash, even though the file does not exist.
	tarCache.add(SystemFilename(tempdir + "/dne"))
	if tf, ok := tarCache.currentTarfile[tempdir]; ok && tf.Size() != 0 {
		t.Error("We added a nonexistent file to the tarCache.")
	}
}
