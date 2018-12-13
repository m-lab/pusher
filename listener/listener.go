// Package listener provides an interface to an inotify-based system for
// watching a directory and its subdirectories for file close and file move
// events.
package listener

import (
	"context"
	"log"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"

	"github.com/m-lab/pusher/tarfile"
	"github.com/rjeczalik/notify"
)

// Set up prometheus metrics.
var (
	pusherFileEventCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_file_events_total",
			Help: "How many file events have we heard.",
		},
		[]string{"type"},
	)
	pusherFileEventErrorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pusher_file_event_errors_total",
			Help: "How many file event errors we have encountered.",
		},
		[]string{"type"},
	)
	// Allow mocking of os.Open to test error cases.
	osOpen = os.Open
)

func init() {
	prometheus.MustRegister(pusherFileEventCount)
	prometheus.MustRegister(pusherFileEventErrorCount)
}

// Listener contains all member variables required for the state of a running
// file listener.
type Listener struct {
	events      chan notify.EventInfo
	fileChannel chan<- tarfile.LocalDataFile
}

// Create and set up an inotify watcher on the directory and its
// subdirectories.  File events will be converted into `tarcache.LocalDataFile`
// structs and pointers to those structs will sent to the passed-in channel.
func Create(directory string, fileChannel chan<- tarfile.LocalDataFile) (*Listener, error) {
	listener := &Listener{
		events:      make(chan notify.EventInfo, 1000000),
		fileChannel: fileChannel,
	}
	// "..." is the special syntax that means "also watch all subdirectories".
	if err := notify.Watch(directory+"/...", listener.events, notify.InCloseWrite|notify.InMovedTo); err != nil {
		return nil, err
	}
	return listener, nil
}

// ListenForever listens for listen for FS events and sends them along the fileChannel until Stop is called.
func (l *Listener) ListenForever(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			notify.Stop(l.events)
			return
		case ei := <-l.events:
			source := "unknown"
			sysinfo := ei.Sys().(*unix.InotifyEvent)
			if sysinfo.Mask&unix.IN_CLOSE_WRITE != 0 {
				source = "closewrite"
			}
			if sysinfo.Mask&unix.IN_MOVED_TO != 0 {
				source = "movedto"
			}
			pusherFileEventCount.WithLabelValues(source).Inc()
			if !isOpenable(ei.Path()) {
				log.Printf("Could not open file for event: %v\n", ei)
				continue
			}
			l.fileChannel <- tarfile.LocalDataFile(ei.Path())
		}
	}

}

func isOpenable(path string) bool {
	_, err := osOpen(path)
	if err != nil {
		pusherFileEventErrorCount.WithLabelValues("open").Inc()
		return false
	}
	return true
}
