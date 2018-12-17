package tarfile_test

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/tarfile"
)

func TestLint(t *testing.T) {
	for _, badString := range []string{
		"/gfdgf/../fsdfds/data.txt",
		"file.txt; rm -Rf *",
		"dir/.gz",
		"dir/.../file.gz",
		"dir/only_a_dir/",
		"ndt/2009/03/ab/file.gz",
	} {
		if tarfile.InternalFilename(badString).Lint() == nil {
			t.Errorf("Should have had a lint error on %q", badString)
		}
	}
	for _, goodString := range []string{
		"ndt/2009/03/13/file.gz",
		"experiment_2/2013/01/01/subdirectory/file.tgz",
	} {
		if warning := tarfile.InternalFilename(goodString).Lint(); warning != nil {
			t.Errorf("Linter gave warning %v on %q", warning, goodString)
		}
	}
}
func TestSubdir(t *testing.T) {
	for _, test := range []struct{ in, out string }{
		{in: "2009/01/01/tes/", out: "2009/01/01"},
		{in: "2009/01/test", out: "2009/01"},
		{in: "2009/test", out: "2009"},
		{in: "test", out: ""},
		{in: "2009/01/01/subdir/test", out: "2009/01/01"},
	} {
		out := tarfile.InternalFilename(test.in).Subdir()
		if out != test.out {
			t.Errorf("The subdirectory should have been %q but was %q", test.out, out)
		}
	}
}

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
	tf := tarfile.New(tmp)
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
	tf := tarfile.New("")
	tf.UploadAndDelete(nil) // If this doesn't crash, then the test passes.
}

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

func TestUploadAndDelete(t *testing.T) {
	tmp, err := ioutil.TempDir("", "tarfile.TestUploadAndDelete")
	rtx.Must(err, "Could not create temp dir")
	defer os.RemoveAll(tmp)
	oldDir, err := os.Getwd()
	rtx.Must(err, "Could not get working directory")
	rtx.Must(os.Chdir(tmp), "Could not chdir to the tempdir")
	defer os.Chdir(oldDir)

	ioutil.WriteFile("tinyfile", []byte("abcdefgh"), os.FileMode(0666))
	f, err := os.Open("tinyfile")
	rtx.Must(err, "Could not open file we just wrote")
	ioutil.WriteFile("disappearing", []byte("abcdefgh"), os.FileMode(0666))
	f2, err := os.Open("disappearing")
	rtx.Must(err, "Could not open file we just wrote")
	tf := tarfile.New("")
	timerFactory := func(string) *time.Timer { return time.NewTimer(time.Hour) }
	tf.Add("tinyfile", f, timerFactory)
	tf.Add("disappearing", f2, timerFactory)
	rtx.Must(os.Remove("disappearing"), "Could not delete file")
	tf.UploadAndDelete(&fakeUploader{})
}
