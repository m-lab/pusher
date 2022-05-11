package finder_test

import (
	"context"
	"errors"
	"io/fs"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m-lab/go/memoryless"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/filename"
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
	// Set up the directories.
	//
	// An old, empty directory.
	rtx.Must(os.Mkdir(tempdir+"/old_empty_dir", 0750), "Mkdir failed")
	newtime = time.Now().Add(time.Duration(-26) * time.Hour)
	rtx.Must(os.Chtimes(tempdir+"/old_empty_dir", newtime, newtime), "Chtimes failed")
	// An old directory, but not empty.
	rtx.Must(os.Mkdir(tempdir+"/old_not_empty_dir", 0750), "Mkdir failed")
	newtime = time.Now().Add(time.Duration(-30) * time.Hour)
	rtx.Must(os.Chtimes(tempdir+"/old_not_empty_dir", newtime, newtime), "Chtimes failed")
	rtx.Must(ioutil.WriteFile(tempdir+"/old_not_empty_dir/test_file", []byte("data\n"), 0644), "WriteFile failed")
	newtime = time.Now().Add(time.Duration(-27) * time.Hour)
	rtx.Must(os.Chtimes(tempdir+"/old_not_empty_dir/test_file", newtime, newtime), "Chtimes failed")
	// A new directory.
	rtx.Must(os.Mkdir(tempdir+"/new_dir", 0750), "Mkdir failed")
	// Set up the receiver channel.
	foundFiles := make(chan filename.System)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := memoryless.Config{
		Min:      time.Microsecond,
		Expected: time.Microsecond,
		Max:      time.Microsecond,
	}
	go finder.FindForever(ctx, "test", filename.System(tempdir), time.Duration(6)*time.Hour, foundFiles, c)
	localfiles := []filename.System{
		<-foundFiles,
		<-foundFiles,
		<-foundFiles,
	}
	// Test files.
	if len(localfiles) != 3 {
		t.Errorf("len(localfiles) (%d) != 2", len(localfiles))
	}
	if string(localfiles[0]) != tempdir+"/old_not_empty_dir/test_file" {
		t.Errorf("wrong name[1]: %s", localfiles[0])
	}
	if string(localfiles[1]) != tempdir+"/oldest_file" {
		t.Errorf("wrong name[1]: %s", localfiles[0])
	}
	if string(localfiles[2]) != tempdir+"/next_oldest_file" {
		t.Errorf("wrong name[2]: %s", localfiles[1])
	}
	// Test directories.
	if _, err = os.Stat(tempdir + "/old_empty_dir"); errors.Is(err, fs.ErrExist) {
		t.Errorf("Directory %s/old_empty_dir exists, but shouldn't", tempdir)
	}
	if _, err = os.Stat(tempdir + "/old_not_empty_dir"); errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Directory %s/old_not_empty_dir does not exist, but should", tempdir)
	}
	if _, err = os.Stat(tempdir + "/new_dir"); errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Directory %s/new_dir does not exist, but should", tempdir)
	}
}

func TestFindForeverNoDirExists(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tempdir, err := ioutil.TempDir("/tmp", "find_file_test")
	defer os.RemoveAll(tempdir)
	rtx.Must(err, "Could not set up temp dir")
	c := memoryless.Config{
		Min:      time.Millisecond,
		Expected: time.Millisecond,
		Max:      time.Millisecond,
	}
	go finder.FindForever(ctx, "dne", "/tmp/dne", time.Duration(time.Millisecond), nil, c)
	time.Sleep(1 * time.Second)
	// If the finder doesn't crash on a bad directory, then it's a success.
}
