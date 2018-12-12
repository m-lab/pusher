package namer_test

import (
	"testing"
	"time"

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
			out:  "exp/monkey/20180506T150102.044001Z-mlab6-lga0t-exp.tgz",
		},
		{
			date: time.Date(2008, 1, 1, 0, 0, 0, 0, time.UTC),
			dir:  "2008/01/01",
			out:  "exp/2008/01/01/20080101T000000.000000Z-mlab6-lga0t-exp.tgz",
		},
	}
	namer, err := namer.New("exp", "mlab6.lga0t")
	if err != nil {
		t.Fatal("Failed to create new namer")
	}
	for _, test := range tests {
		if out := namer.ObjectName(test.dir, test.date); out != test.out {
			t.Errorf("%q != %q (input: %v, %v)", out, test.out, test.dir, test.date)
		}
	}
}

func TestNew(t *testing.T) {
	fakeDate := time.Date(2011, 3, 4, 12, 45, 0, 0, time.UTC)
	tests := []struct {
		name        string
		nodeName    string
		wantObjName string
		wantErr     bool
	}{
		{
			name:        "success",
			nodeName:    "mlab5.abc0t.measurement-lab.org",
			wantObjName: "fake-experiment/2011/03/04/20110304T124500.000000Z-mlab5-abc0t-fake-experiment.tgz",
		},
		{
			name:     "failure-machine-too-short",
			nodeName: "mlab.abc0t.measurement-lab.org",
			wantErr:  true,
		},
		{
			name:     "failure-site-too-short",
			nodeName: "mlab5.abc.measurement-lab.org",
			wantErr:  true,
		},
		{
			name:     "failure-nodename-has-too-few-fields",
			nodeName: "this-is-not-a-hostname",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := namer.New("fake-experiment", tt.nodeName)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			obj := got.ObjectName("2011/03/04", fakeDate)
			if obj != tt.wantObjName {
				t.Errorf("ObjectName() got = %q, want %q", obj, tt.wantObjName)
				return
			}
		})
	}
}
