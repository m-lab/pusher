package listener

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m-lab/pusher/tarcache"
	"github.com/rjeczalik/notify"
	"golang.org/x/sys/unix"
)

func failOnOpen(name string) (*os.File, error) {
	return nil, fmt.Errorf("A mock error for %s", name)
}

func TestOsOpenFailure(t *testing.T) {
	osOpen = failOnOpen
	defer func() { osOpen = os.Open }()
	ptr, err := convertEventInfoToLocalDataFile("")
	if ptr != nil || err == nil {
		t.Error("convertEventInfo should have had an error but did not")
	}
}

func openFileThatFailsOnStat(name string) (*os.File, error) {
	return nil, nil
}

func TestStatFailure(t *testing.T) {
	osOpen = openFileThatFailsOnStat
	defer func() { osOpen = os.Open }()
	ptr, err := convertEventInfoToLocalDataFile("")
	if ptr != nil || err == nil {
		t.Error("convertEventInfo should have had an error but did not")
	}
}

type MockEventInfo struct{}

func (m MockEventInfo) Event() notify.Event {
	return notify.InCloseWrite
}

func (m MockEventInfo) Path() string {
	return ""
}

func (m MockEventInfo) Sys() interface{} {
	return &unix.InotifyEvent{}
}

func TestBadEvent(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "TestBadEvent.")
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	defer os.RemoveAll(dir)
	ldfChan := make(chan *tarcache.LocalDataFile)
	l, err := Create(dir, ldfChan)
	defer l.Stop()
	go l.ListenForever()
	l.events <- &MockEventInfo{}
	time.Sleep(250 * time.Millisecond)
	// No crash == test success
}
