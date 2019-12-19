package tarcache_test

import (
	"context"
	"crypto/rand"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/m-lab/go/memoryless"

	"github.com/m-lab/go/flagx"

	"github.com/m-lab/go/bytecount"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/filename"
	"github.com/m-lab/pusher/tarcache"
)

type fakeUploader struct {
	calls int
	mutex sync.Mutex
}

func (f *fakeUploader) Upload(_ filename.System, _ []byte) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.calls++
	return nil
}

func (f *fakeUploader) Calls() int {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.calls
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
	config := memoryless.Config{
		Min:      100 * time.Millisecond,
		Expected: 100 * time.Millisecond,
		Max:      100 * time.Millisecond,
	}
	tarCache, channel := tarcache.New(filename.System(tempdir), "test", &flagx.KeyValue{}, bytecount.ByteCount(1*bytecount.Kilobyte), config, uploader)
	// Add the small file, which should not trigger an upload.
	tinyFile := filename.System("a/b/tinyfile")
	otherTinyFile := filename.System("c/d/tinyfile")
	ctx := context.Background()
	go tarCache.ListenForever(ctx, ctx)
	channel <- tinyFile
	channel <- otherTinyFile
	if uploader.Calls() != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Sleep to cause a timeout.
	time.Sleep(250 * time.Millisecond)
	// Verify that the timer fired twice - once for each subdirectory.
	if uploader.Calls() != 2 {
		t.Error("uploader.calls should be 2 ", uploader.calls)
	}

	// Do it again to verify that the timer setup doesn't break after a single use.
	uploader.calls = 0
	// With no files added, the timer should not fire.
	time.Sleep(time.Duration(250 * time.Millisecond))
	if uploader.Calls() != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Create a tiny file and add it.
	ioutil.WriteFile("tiny2", []byte("12345678"), os.FileMode(0666))
	tiny2File := filename.System("tiny2")
	channel <- tiny2File
	if uploader.Calls() != 0 {
		t.Error("uploader.calls should be zero ", uploader.calls)
	}
	// Wait for the timer to fire.
	time.Sleep(time.Duration(250 * time.Millisecond))
	if uploader.Calls() != 1 {
		t.Error("uploader.calls should be one ", uploader.calls)
	}
}

func TestContextCancellation(t *testing.T) {
	tempdir, err := ioutil.TempDir("/tmp", "tarcache.TestContextCancellation")
	rtx.Must(err, "Could not create tempdir")
	defer os.RemoveAll(tempdir)

	oldDir, err := os.Getwd()
	rtx.Must(err, "Could not get working directory")
	rtx.Must(os.Chdir(tempdir), "Could not chdir to the tempdir")
	defer os.Chdir(oldDir)

	// Set up a tarcache with timeouts and bytecounts that ensure it will not fire with a small short test.
	uploader := fakeUploader{}
	config := memoryless.Config{
		Min:      100 * time.Hour,
		Expected: 100 * time.Hour,
		Max:      100 * time.Hour,
	}
	tarCache, fileChan := tarcache.New(filename.System("/tmp"), "test", &flagx.KeyValue{}, bytecount.ByteCount(1*bytecount.Gigabyte), config, &uploader)
	killCtx, killCancel := context.WithCancel(context.Background())
	termCtx, termCancel := context.WithCancel(killCtx)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		tarCache.ListenForever(termCtx, killCtx)
		wg.Done()
	}()

	// Wait a bit so that all channels are set up in the goroutine.
	time.Sleep(100 * time.Millisecond)

	// Add some files.
	rtx.Must(os.MkdirAll("2010/04/23", 0777), "Could not make directories")
	rtx.Must(ioutil.WriteFile("2010/04/23/testdata.txt", []byte("12345"), 0666), "Could not write test data")
	fileChan <- filename.System("2010/04/23/testdata.txt")

	rtx.Must(os.MkdirAll("2010/04/24", 0777), "Could not make directories")
	rtx.Must(ioutil.WriteFile("2010/04/24/testdata.txt", []byte("12345"), 0666), "Could not write test data")
	fileChan <- filename.System("2010/04/24/testdata.txt")

	// Wait a bit so that all channel input can get processed.
	time.Sleep(10 * time.Millisecond)

	// Verify that nothing has been uploaded.
	if uploader.calls != 0 {
		t.Errorf("Should have uploaded 0 times, not %d", uploader.calls)
	}

	// Cancel things with the first context to cause an upload and then wait for the cancellation to take effect.
	termCancel()
	time.Sleep(100 * time.Millisecond)

	// Verify that something has been uploaded in each of the subdirectories
	if uploader.Calls() != 2 {
		t.Errorf("Should have uploaded 2 times, not %d", uploader.calls)
	}

	// Add another file.
	rtx.Must(os.MkdirAll("2010/04/23", 0777), "Could not make directories")
	rtx.Must(ioutil.WriteFile("2010/04/23/testdata2.txt", []byte("12345"), 0666), "Could not write test data")
	fileChan <- filename.System("2010/04/23/testdata2.txt")

	// Wait a bit so that all channel input can get processed.
	time.Sleep(10 * time.Millisecond)

	// Cancel the second context to cause another upload and loop termination.
	killCancel()

	// Wait for the cancellation to terminate the loop.
	wg.Wait()

	// Verify that one more upload happened.
	if uploader.Calls() != 3 {
		t.Errorf("Should have uploaded 3 times in all, not %d", uploader.calls)
	}
}

func TestChannelCloseCancellation(t *testing.T) {
	uploader := fakeUploader{}
	config := memoryless.Config{
		Min:      100 * time.Millisecond,
		Expected: 100 * time.Millisecond,
		Max:      100 * time.Millisecond,
	}
	tarCache, inputChannel := tarcache.New(filename.System("/tmp"), "test", &flagx.KeyValue{}, bytecount.ByteCount(1*bytecount.Kilobyte), config, &uploader)
	ctx := context.Background()
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(inputChannel)
	}()
	// If this doesn't actually listen forever, then this test is a success.
	tarCache.ListenForever(ctx, ctx)
}
