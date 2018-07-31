package namer_test

import (
	"testing"
	"time"

	"github.com/m-lab/pusher/namer"
)

func TestFilenameGeneration(t *testing.T) {
	tests := []struct {
		in  time.Time
		out string
	}{
		{
			in:  time.Date(2018, 5, 6, 15, 1, 2, 44000000, time.UTC),
			out: "exp/2018/05/06/20180506T150102.044Z-mlab6-lga0t-exp.tgz",
		},
		{
			in:  time.Date(2008, 1, 1, 0, 0, 0, 0, time.UTC),
			out: "exp/2008/01/01/20080101T000000.000Z-mlab6-lga0t-exp.tgz",
		},
	}
	namer := namer.New("exp", "mlab6", "lga0t")
	for _, test := range tests {
		if out := namer.ObjectName(test.in); out != test.out {
			t.Errorf("%q != %q (input: %v)", out, test.out, test.in)
		}
	}
}
