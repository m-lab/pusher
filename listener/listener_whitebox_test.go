package listener

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/filename"
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
	rtx.Must(err, "Could not create dir")
	defer os.RemoveAll(dir)
	ldfChan := make(chan filename.System)
	l, err := Create(filename.System(dir), ldfChan)
	rtx.Must(err, "Could not create listener")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.ListenForever(ctx)
	l.events <- &MockEventInfo{}
	time.Sleep(250 * time.Millisecond)
	// No crash == test success
}
