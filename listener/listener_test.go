package listener_test

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/filename"
	"github.com/m-lab/pusher/listener"
)

func TestListenForClose(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "TestListenForClose.")
	rtx.Must(err, "Could not create tempdir")
	defer os.RemoveAll(dir)
	ldfChan := make(chan filename.System)
	l, err := listener.Create(filename.System(dir), ldfChan)
	rtx.Must(err, "Could not create listener")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.ListenForever(ctx)
	rtx.Must(ioutil.WriteFile(dir+"/testfile", []byte("test"), 0777), "Could not write file")
	ldf := <-ldfChan
	if !strings.HasSuffix(string(ldf), "/testfile") || !strings.HasPrefix(string(ldf), "/tmp/TestListenForClose.") {
		t.Errorf("Bad filename: %v\n", ldf)
	}
}

func TestListenForMove(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "TestListenForMove.")
	rtx.Must(err, "Could not create tempdir")
	defer os.RemoveAll(dir)
	os.Mkdir(dir+"/subdir", 0777)
	rtx.Must(ioutil.WriteFile(dir+"/testfile", []byte("test"), 0777), "Could not write file")
	ldfChan := make(chan filename.System)
	l, err := listener.Create(filename.System(dir+"/subdir"), ldfChan)
	rtx.Must(err, "Could not create listener")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.ListenForever(ctx)
	rtx.Must(os.Rename(dir+"/testfile", dir+"/subdir/testfile"), "Could not rename")
	ldf := <-ldfChan
	if !strings.HasSuffix(string(ldf), "/subdir/testfile") || !strings.HasPrefix(string(ldf), "/tmp/TestListenForMove.") {
		t.Errorf("Bad filename: %v\n", ldf)
	}
}

func TestListenInSubdir(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "TestListenInSubdir.")
	rtx.Must(err, "Could not create tempdir")
	defer os.RemoveAll(dir)
	os.Mkdir(dir+"/subdir", 0777)
	ldfChan := make(chan filename.System)
	l, err := listener.Create(filename.System(dir+"/subdir"), ldfChan)
	rtx.Must(err, "Could not create listener")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.ListenForever(ctx)
	os.MkdirAll(dir+"/subdir/sub1/sub2", 0777)
	// Sleep to allow the subdirectory listener to be established.
	time.Sleep(100 * time.Millisecond)
	rtx.Must(ioutil.WriteFile(dir+"/subdir/sub1/sub2/testfile", []byte("testdata"), 0777), "Could not write file")
	ldf := <-ldfChan
	if dir+"/subdir/sub1/sub2/testfile" != string(ldf) {
		t.Errorf("Bad filename: %v\n", ldf)
	}
}

func TestCreateOnBadDir(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "TestCreateOnBadDir.")
	rtx.Must(err, "Could not create tempdir")
	defer os.RemoveAll(dir)
	ldfChan := make(chan filename.System)
	l, err := listener.Create(filename.System(dir+"/doesnotexist"), ldfChan)
	if l != nil || err == nil {
		t.Error("Should have had an error")
	}
}

func TestReadCloseWontNotify(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "TestReadCloseWontNotify.")
	rtx.Must(err, "Could not create tempdir")
	defer os.RemoveAll(dir)
	rtx.Must(ioutil.WriteFile(dir+"/testfile", []byte("test"), 0777), "Could not write file")
	ldfChan := make(chan filename.System)
	l, err := listener.Create(filename.System(dir), ldfChan)
	rtx.Must(err, "Could not create listener")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.ListenForever(ctx)
	f, err := os.Open(dir + "/testfile")
	rtx.Must(err, "Could not open file")
	f.Read(make([]byte, 4))
	rtx.Must(f.Close(), "Could not close file")
	select {
	case <-ldfChan:
		t.Error("A read close should not cause an event")
	case <-time.NewTimer(100 * time.Millisecond).C:
	}
}
