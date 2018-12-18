package tarfile_test

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/filename"
	"github.com/m-lab/pusher/tarfile"
)

var timerFactoryCalls = 0

func nilTimerFactory(string) *time.Timer {
	timerFactoryCalls += 1
	return nil
}

type unstatAbleFilePointer struct {
	*os.File
}

func (_ unstatAbleFilePointer) Stat() (os.FileInfo, error) {
	return nil, errors.New("Can't stat this")
}

type unreadableioFile struct {
	*os.File
}

func (_ unreadableioFile) Read(_ []byte) (int, error) {
	return 0, errors.New("This can't be read")
}

type tooLongioFile struct {
	*os.File
}

func (_ tooLongioFile) Read(buf []byte) (int, error) {
	return len(buf), nil
}

func TestAdd(t *testing.T) {
	tmp, err := ioutil.TempDir("", "tarfile.TestAdd")
	rtx.Must(err, "Could not create temp dir")
	defer os.RemoveAll(tmp)
	oldDir, err := os.Getwd()
	rtx.Must(err, "Could not get working directory")
	rtx.Must(os.Chdir(tmp), "Could not chdir to the tempdir")
	defer os.Chdir(oldDir)
	timerFactoryCalls = 0
	tf := tarfile.New("test", "")
	ioutil.WriteFile("tinyfile", []byte("abcdefgh"), os.FileMode(0666))
	if tf.Size() != 0 {
		t.Errorf("Tarfile size is nonzero before anything is added to it")
	}
	f, err := os.Open("tinyfile")
	rtx.Must(err, "Could not open tinyfile")
	tf.Add("tinyfile", f, nilTimerFactory)
	if tf.Size() == 0 {
		t.Errorf("Tarfile size is zero after something is added to it")
	}
	if timerFactoryCalls != 1 {
		t.Error("No timer was created")
	}
	oldsize := tf.Size()
	tf.Add("tinyfile", f, nilTimerFactory)
	tf.Add("tinyfile2", unstatAbleFilePointer{f}, nilTimerFactory)
	tf.Add("tinyfile3", unreadableioFile{f}, nilTimerFactory)
	tf.Add("tinyfile4", tooLongioFile{f}, nilTimerFactory)
	if tf.Size() != oldsize {
		t.Error("Bad files should not increase the size of the tarfile")
	}
}
func TestUploadAndDeleteOnEmpty(t *testing.T) {
	tf := tarfile.New("test", "")
	tf.UploadAndDelete(nil) // If this doesn't crash, then the test passes.
}

type fakeUploader struct {
	contents         []byte
	calls            int
	requestedRetries int
	expectedDir      string
}

func (f *fakeUploader) Upload(dir filename.System, contents []byte) error {
	if f.expectedDir != "" && string(dir) != f.expectedDir {
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

func TestUploadAndDelete(t *testing.T) {
	tmp, err := ioutil.TempDir("", "tarfile.TestUploadAndDelete")
	rtx.Must(err, "Could not create temp dir")
	defer os.RemoveAll(tmp)
	oldDir, err := os.Getwd()
	rtx.Must(err, "Could not get working directory")
	rtx.Must(os.Chdir(tmp), "Could not chdir to the tempdir")
	defer os.Chdir(oldDir)
	// A normal file.
	ioutil.WriteFile("tinyfile", []byte("abcdefgh"), os.FileMode(0666))
	f, err := os.Open("tinyfile")
	rtx.Must(err, "Could not open file we just wrote")
	// This file disappears before it can be removed by the tarfile, ensuring that
	// files that disappear don't cause problems..
	ioutil.WriteFile("disappearing", []byte("abcdefgh"), os.FileMode(0666))
	f2, err := os.Open("disappearing")
	rtx.Must(err, "Could not open file we just wrote")
	rtx.Must(os.Remove("disappearing"), "Could not delete file")
	tf := tarfile.New("test", "")
	timerFactory := func(string) *time.Timer { return time.NewTimer(time.Hour) }
	tf.Add("tinyfile", f, timerFactory)
	tf.Add("disappearing", f2, timerFactory)
	tf.UploadAndDelete(&fakeUploader{})
}
