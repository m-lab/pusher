package filename_test

import (
	"testing"

	"github.com/m-lab/pusher/filename"
)

func TestLint(t *testing.T) {
	for _, badString := range []string{
		"/gfdgf/../fsdfds/data.txt",
		"file.txt; rm -Rf *",
		"dir/.gz",
		"dir/.../file.gz",
		"dir/only_a_dir/",
		"ndt/2009/03/ab/file.gz",
	} {
		if filename.Internal(badString).Lint() == nil {
			t.Errorf("Should have had a lint error on %q", badString)
		}
	}
	for _, goodString := range []string{
		"ndt/2009/03/13/file.gz",
		"experiment_2/2013/01/01/subdirectory/file.tgz",
	} {
		if warning := filename.Internal(goodString).Lint(); warning != nil {
			t.Errorf("Linter gave warning %v on %q", warning, goodString)
		}
	}
}
func TestSubdir(t *testing.T) {
	for _, test := range []struct{ in, out string }{
		{in: "2009/01/01/tes/", out: "2009/01/01"},
		{in: "2009/01/test", out: "2009/01"},
		{in: "2009/test", out: "2009"},
		{in: "test", out: ""},
		{in: "2009/01/01/subdir/test", out: "2009/01/01"},
	} {
		out := filename.Internal(test.in).Subdir()
		if out != test.out {
			t.Errorf("The subdirectory should have been %q but was %q", test.out, out)
		}
	}
}
