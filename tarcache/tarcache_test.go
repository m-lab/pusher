package tarcache

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m-lab/pusher/bytecount"
	"github.com/m-lab/pusher/fileinfo"
)

type fakeUploader struct {
	contents []byte
	calls	 int
}

func (f *fakeUploader) Upload(contents *bytes.Buffer) {
	f.contents = contents.Bytes()
	f.calls++
}

func TestAdd(t *testing.T) {
	tempdir, err := ioutil.TempDir("/tmp", "tarcache.TestAdd")
	defer os.RemoveAll(tempdir)
	if err != nil {
		t.Error(err)
		return
	}
	tarCache := New(tempdir, bytecount.ByteCount(1*bytecount.Kilobyte), time.Duration(1*time.Hour), nil)
	if err = ioutil.WriteFile(tempdir + "tinyfile", []byte("contents"), os.FileMode(0666)); err != nil {
		t.Error(err)
		return
	}
	if tarCache.tarFileContents.Len() != 0 {
		t.Errorf("The file should be of zero length and is not (%d != 0)", tarCache.tarFileContents.Len())
	}
	fileObject, _ := os.Open(tempdir + "tinyfile")
	fileStat, _ := fileObject.Stat()
	file := fileinfo.LocalDataFile{
		AbsoluteFileName: tempdir + "tinyfile",
		Info: fileStat,
	}
	tarCache.Add(&file)
	if tarCache.tarFileContents.Len() == 0 {
		t.Errorf("The file should be of nonzero length and is not (%d == 0)", tarCache.tarFileContents.Len())
	}
}
