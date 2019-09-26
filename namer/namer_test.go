package namer_test

import (
	"testing"
	"time"

	"github.com/m-lab/pusher/filename"
	"github.com/m-lab/pusher/namer"
)

func TestFilenameGeneration(t *testing.T) {
	tests := []struct {
		date time.Time
		dir  string
		out  string
	}{
		{
			date: time.Date(2018, 5, 6, 15, 1, 2, 44001000, time.UTC),
			dir:  "monkey",
			out:  "exp/summary/monkey/20180506T150102.044001Z-summary-mlab6-lga0t-exp.tgz",
		},
		{
			date: time.Date(2008, 1, 1, 0, 0, 0, 0, time.UTC),
			dir:  "2008/01/01",
			out:  "exp/summary/2008/01/01/20080101T000000.000000Z-summary-mlab6-lga0t-exp.tgz",
		},
	}
	namer := namer.New("summary", "exp", "mlab6-lga0t")
	for _, test := range tests {
		if out := namer.ObjectName(filename.System(test.dir), test.date); out != test.out {
			t.Errorf("%q != %q (input: %v, %v)", out, test.out, test.dir, test.date)
		}
	}
}
