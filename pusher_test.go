package main

import "testing"

func Test_mlabNameToNodeName(t *testing.T) {
	tests := []struct {
		name     string
		nodeName string
		want     string
		wantErr  bool
	}{
		{
			name:     "okay-name",
			nodeName: "mlab1.abc0t.measurement-lab.org",
			want:     "mlab1-abc0t",
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

		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mlabNameToNodeName(tt.nodeName)
			if (err != nil) != tt.wantErr {
				t.Errorf("mlabNameToNodeName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("mlabNameToNodeName() = %v, want %v", got, tt.want)
			}
		})
	}
}
