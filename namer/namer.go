// Package namer provides a tool for creating archival filenames from a timestamp.
package namer

import (
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/m-lab/pusher/filename"
)

// Namer creates tarfile names from timestamps.  The name does not include the bucket.
type Namer interface {
	ObjectName(filename.System, time.Time) string
}

// This is a specific namer used for M-Lab experiments.
type namer struct {
	datatype, experiment, node, site string
}

// New creates a new Namer for the given experiment, node, and site.
func New(datatype, experiment, nodeName string) (Namer, error) {
	// Extract M-Lab machine (mlab5) and site (abc0t) names from node FQDN (mlab5.abc0t.measurement-lab.org).
	fields := strings.SplitN(nodeName, ".", 3)
	if len(fields) < 2 {
		return nil, fmt.Errorf("node name is missing machine and site fields: %s", nodeName)
	}
	if len(fields[0]) != 5 || len(fields[1]) != 5 {
		return nil, fmt.Errorf("machine and site names should have only five characters, e.g. mlab5.abc0t: %s.%s",
			fields[0], fields[1])
	}
	return namer{
		datatype:   datatype,
		experiment: experiment,
		node:       fields[0],
		site:       fields[1],
	}, nil
}

// ObjectName returns a string (with a leading '/') representing the correct
// filename for an uploaded tarfile in a bucket.
func (n namer) ObjectName(subdir filename.System, t time.Time) string {
	timestring := t.Format("20060102T150405.000000Z")
	return path.Join(n.experiment, n.datatype, string(subdir), timestring+"-"+n.datatype+"-"+n.node+"-"+n.site+"-"+n.experiment+".tgz")
}
