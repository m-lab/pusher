// Package listener provides an interface to an inotify-based system for
// watching a directory and its subdirectories for file close and file move
// events.
package listener

import (
	"log"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sys/unix"

	"github.com/m-lab/pusher/tarcache"
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
	stopper     chan int
	fileChannel chan<- *tarcache.LocalDataFile
}

// Create and set up an inotify watcher on the directory and its
// subdirectories.  File events will be converted into `tarcache.LocalDataFile`
// structs and pointers to those structs will sent to the passed-in channel.
func Create(directory string, fileChannel chan<- *tarcache.LocalDataFile) (*Listener, error) {
	listener := &Listener{
		events:      make(chan notify.EventInfo, 1000000),
		stopper:     make(chan int),
		fileChannel: fileChannel,
	}
	// "..."" is the special syntax that means "also watch all subdirectories".
	if err := notify.Watch(directory+"/...", listener.events, notify.InCloseWrite|notify.InMovedTo); err != nil {
		return nil, err
	}
	return listener, nil
}

// Stop listening to filesystem events
func (l *Listener) Stop() {
	close(l.stopper)
}

// ListenForever listens for listen for FS events and sends them along the fileChannel until Stop is called.
func (l *Listener) ListenForever() {
	for {
		select {
		case <-l.stopper:
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
			ldf, err := convertEventInfoToLocalDataFile(ei.Path())
			if err != nil {
				log.Printf("Could not create file for event: %v\n", ei)
				continue
			}
			l.fileChannel <- ldf
		}
	}

}

func convertEventInfoToLocalDataFile(path string) (*tarcache.LocalDataFile, error) {
	file, err := osOpen(path)
	if err != nil {
		pusherFileEventErrorCount.WithLabelValues("open").Inc()
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		// It is a rare file on a strange/broken filesystem that can be open()ed, but
		// not fstat()ed.  This should hopefully never happen in practice.
		pusherFileEventErrorCount.WithLabelValues("stat").Inc()
		return nil, err
	}
	return &tarcache.LocalDataFile{
		AbsoluteFileName: path,
		Info:             info,
	}, nil
}
