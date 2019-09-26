// Package namer provides a tool for creating archival filenames from a timestamp.
package namer

import (
	"path"
	"time"

	"github.com/m-lab/pusher/filename"
)

// Namer creates tarfile names from timestamps.  The name does not include the bucket.
type Namer interface {
	ObjectName(filename.System, time.Time) string
}

// This is a specific namer used for M-Lab experiments.
type namer struct {
	datatype, experiment, node string
}

// New creates a new Namer for the given experiment, node, and site.
func New(datatype, experiment, nodeName string) Namer {
	return namer{
		datatype:   datatype,
		experiment: experiment,
		node:       nodeName,
	}
}

// ObjectName returns a string (with a leading '/') representing the correct
// filename for an uploaded tarfile in a bucket.
func (n namer) ObjectName(subdir filename.System, t time.Time) string {
	timestring := t.Format("20060102T150405.000000Z")
	return path.Join(n.experiment, n.datatype, string(subdir), timestring+"-"+n.datatype+"-"+n.node+"-"+n.experiment+".tgz")
}
