package finder_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m-lab/pusher/finder"

	"github.com/m-lab/pusher/tarcache"
)

func TestFindForever(t *testing.T) {
	// Set up the files.
	tempdir, _ := ioutil.TempDir("/tmp", "find_file_test")
	defer os.RemoveAll(tempdir)
	file1, _ := os.Create(tempdir + "/oldest_file")
	_, _ = file1.WriteString("data\n")
	_ = file1.Close()
	newtime := time.Now().Add(time.Duration(-13) * time.Hour)
	_ = os.Chtimes(tempdir+"/oldest_file", newtime, newtime)
	file2, _ := os.Create(tempdir + "/next_oldest_file")
	_, _ = file2.WriteString("moredata\n")
	_ = file2.Close()
	newtime = time.Now().Add(time.Duration(-12) * time.Hour)
	_ = os.Chtimes(tempdir+"/next_oldest_file", newtime, newtime)
	// Set up the receiver channel.
	foundFiles := make(chan *tarcache.LocalDataFile)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go finder.FindForever(ctx, tempdir, time.Duration(6)*time.Hour, foundFiles, 1*time.Nanosecond)
	localfiles := []*tarcache.LocalDataFile{
		<-foundFiles,
		<-foundFiles,
	}
	if len(localfiles) != 2 {
		t.Errorf("len(localfiles) (%d) != 2", len(localfiles))
	}
	if localfiles[0].AbsoluteFileName != tempdir+"/oldest_file" {
		t.Errorf("wrong name[0]: %s", localfiles[0].AbsoluteFileName)
	}
	if localfiles[1].AbsoluteFileName != tempdir+"/next_oldest_file" {
		t.Errorf("wrong name[1]: %s", localfiles[1].AbsoluteFileName)
	}
}

func TestFindForeverNoDirExists(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tempdir, _ := ioutil.TempDir("/tmp", "find_file_test")
	defer os.RemoveAll(tempdir)
	go finder.FindForever(ctx, "/tmp/dne", time.Duration(time.Minute), nil, time.Duration(time.Millisecond))
	time.Sleep(1 * time.Second)
}
