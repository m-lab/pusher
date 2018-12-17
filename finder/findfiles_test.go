package finder_test

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m-lab/pusher/tarcache"

	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/finder"
)

func TestFindForever(t *testing.T) {
	// Set up the files.
	tempdir, err := ioutil.TempDir("/tmp", "find_file_test")
	defer os.RemoveAll(tempdir)
	rtx.Must(err, "Could not set up temp dir")
	// Set up the files
	rtx.Must(ioutil.WriteFile(tempdir+"/oldest_file", []byte("data\n"), 0644), "WriteFile failed")
	newtime := time.Now().Add(time.Duration(-13) * time.Hour)
	rtx.Must(os.Chtimes(tempdir+"/oldest_file", newtime, newtime), "Chtimes failed")
	rtx.Must(ioutil.WriteFile(tempdir+"/next_oldest_file", []byte("moredata\n"), 0644), "WriteFile failed")
	newtime = time.Now().Add(time.Duration(-12) * time.Hour)
	rtx.Must(os.Chtimes(tempdir+"/next_oldest_file", newtime, newtime), "Chtimes failed")
	// Set up the receiver channel.
	foundFiles := make(chan tarcache.SystemFilename)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go finder.FindForever(ctx, tempdir, time.Duration(6)*time.Hour, foundFiles, 1*time.Microsecond)
	localfiles := []tarcache.SystemFilename{
		<-foundFiles,
		<-foundFiles,
	}
	if len(localfiles) != 2 {
		t.Errorf("len(localfiles) (%d) != 2", len(localfiles))
	}
	if string(localfiles[0]) != tempdir+"/oldest_file" {
		t.Errorf("wrong name[0]: %s", localfiles[0])
	}
	if string(localfiles[1]) != tempdir+"/next_oldest_file" {
		t.Errorf("wrong name[1]: %s", localfiles[1])
	}
}

func TestFindForeverNoDirExists(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tempdir, err := ioutil.TempDir("/tmp", "find_file_test")
	defer os.RemoveAll(tempdir)
	rtx.Must(err, "Could not set up temp dir")
	go finder.FindForever(ctx, "/tmp/dne", time.Duration(time.Millisecond), nil, time.Duration(time.Millisecond))
	time.Sleep(1 * time.Second)
	// If the finder doesn't crash on a bad directory, then it's a success.
}
