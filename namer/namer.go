package namer

import (
	"time"
)

// Namer creates tarfile names from timestamps.  The name does not include the bucket.
type Namer interface {
	ObjectName(time.Time) string
}

// This is a specific namer used for M-Lab experiments.
type namer struct {
	experiment, node, site string
}

// New creates a new Namer for the given experiment, node, and site.
func New(experiment string, node string, site string) Namer {
	return namer{
		experiment: experiment,
		node:       node,
		site:       site,
	}
}

// ObjectName returns a string (with a leading '/') representing the correct
// filename for an uploaded tarfile in a bucket.
func (n namer) ObjectName(t time.Time) string {
	timestring := t.Format("2006/01/02/20060102T150405.000Z")
	return ("/" + n.experiment + "/" + timestring + "-" + n.node + "-" + n.site + "-" + n.experiment + ".tgz")
}
