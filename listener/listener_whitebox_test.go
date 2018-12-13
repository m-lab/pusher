package listener

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m-lab/pusher/tarfile"
	"github.com/rjeczalik/notify"
	"golang.org/x/sys/unix"
)

func failOnOpen(name string) (*os.File, error) {
	return nil, fmt.Errorf("A mock error for %s", name)
}

func TestOsOpenFailure(t *testing.T) {
	osOpen = failOnOpen
	defer func() { osOpen = os.Open }()
	if isOpenable("") {
		t.Error("isOpenable should return false")
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
	ldfChan := make(chan tarfile.LocalDataFile)
	l, err := Create(dir, ldfChan)
	if err != nil {
		t.Errorf("%v", err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.ListenForever(ctx)
	l.events <- &MockEventInfo{}
	time.Sleep(250 * time.Millisecond)
	// No crash == test success
}
