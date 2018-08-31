package listener_test

import (
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/pusher/tarcache"

	"github.com/m-lab/pusher/listener"
)

func TestListenForClose(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "TestListenForClose.")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	defer os.RemoveAll(dir)
	ldfChan := make(chan *tarcache.LocalDataFile)
	l, err := listener.Create(dir, ldfChan)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.ListenForever(ctx)
	ioutil.WriteFile(dir+"/testfile", []byte("test"), 0777)
	ldf := <-ldfChan
	if !strings.HasSuffix(ldf.AbsoluteFileName, "/testfile") || !strings.HasPrefix(ldf.AbsoluteFileName, "/tmp/TestListenForClose.") {
		t.Errorf("Bad filename: %q\n", ldf.AbsoluteFileName)
	}
}

func TestListenForMove(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "TestListenForMove.")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	defer os.RemoveAll(dir)
	os.Mkdir(dir+"/subdir", 0777)
	ioutil.WriteFile(dir+"/testfile", []byte("test"), 0777)
	ldfChan := make(chan *tarcache.LocalDataFile)
	l, err := listener.Create(dir+"/subdir", ldfChan)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.ListenForever(ctx)
	err = os.Rename(dir+"/testfile", dir+"/subdir/testfile")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	ldf := <-ldfChan
	if !strings.HasSuffix(ldf.AbsoluteFileName, "/subdir/testfile") || !strings.HasPrefix(ldf.AbsoluteFileName, "/tmp/TestListenForMove.") {
		t.Errorf("Bad filename: %q\n", ldf.AbsoluteFileName)
	}
}

func TestListenInSubdir(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "TestListenInSubdir.")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	defer os.RemoveAll(dir)
	os.Mkdir(dir+"/subdir", 0777)
	ldfChan := make(chan *tarcache.LocalDataFile)
	l, err := listener.Create(dir+"/subdir", ldfChan)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.ListenForever(ctx)
	os.MkdirAll(dir+"/subdir/sub1/sub2", 0777)
	// Sleep to allow the subdirectory listener to be established.
	time.Sleep(100 * time.Millisecond)
	ioutil.WriteFile(dir+"/subdir/sub1/sub2/testfile", []byte("testdata"), 0777)
	ldf := <-ldfChan
	if dir+"/subdir/sub1/sub2/testfile" != ldf.AbsoluteFileName {
		t.Errorf("Bad filename: %q\n", ldf.AbsoluteFileName)
	}
}

func TestCreateOnBadDir(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "TestCreateOnBadDir.")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	defer os.RemoveAll(dir)
	ldfChan := make(chan *tarcache.LocalDataFile)
	l, err := listener.Create(dir+"/doesnotexist", ldfChan)
	if l != nil || err == nil {
		t.Error("Should have had an error")
	}
}
