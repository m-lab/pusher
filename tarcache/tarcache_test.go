package tarcache_test

import (
	"context"
	"crypto/rand"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m-lab/go/bytecount"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/tarcache"
)

type fakeUploader struct {
	calls int
}

func (f *fakeUploader) Upload(dir string, contents []byte) error {
	f.calls++
	return nil
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
	tarCache, channel := tarcache.New(tempdir, bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(100*time.Millisecond), uploader)
	// Add the small file, which should not trigger an upload.
	tinyFile := tarcache.SystemFilename("a/b/tinyfile")
	otherTinyFile := tarcache.SystemFilename("c/d/tinyfile")
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
	tiny2File := tarcache.SystemFilename("tiny2")
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
	tarCache, _ := tarcache.New("/tmp", bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(100*time.Millisecond), &uploader)
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
	tarCache, inputChannel := tarcache.New("/tmp", bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(100*time.Millisecond), &uploader)
	ctx := context.Background()
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(inputChannel)
	}()
	// If this doesn't actually listen forever, then this test is a success.
	tarCache.ListenForever(ctx)
}
