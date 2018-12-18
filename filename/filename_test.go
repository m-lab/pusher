package filename_test

import (
	"testing"

	"github.com/m-lab/pusher/filename"
)

func TestInternal(t *testing.T) {
	for _, test := range []struct{ in, out string }{
		{in: "/var/spool/ndt/summary/2009/01/01/tes/", out: "2009/01/01/tes/"},
		{in: "/var/spool/ndt/summary/2009/01/test", out: "2009/01/test"},
		{in: "/var/spool/ndt/summary/2009/test", out: "2009/test"},
		{in: "/var/spool/ndt/summary/test", out: "test"},
		{in: "/var/spool/ndt/summary/2009/01/01/subdir/test", out: "2009/01/01/subdir/test"},
	} {
		out := filename.System(test.in).Internal(filename.System("/var/spool/ndt/summary/"))
		if string(out) != test.out {
			t.Errorf("The subdirectory should have been %q but was %q", test.out, out)
		}
	}
}

func TestLint(t *testing.T) {
	for _, badString := range []string{
		"/gfdgf/../fsdfds/data.txt",
		"file.txt; rm -Rf *",
		"dir/.gz",
		"dir/.../file.gz",
		"dir/only_a_dir/",
		"ndt/2009/03/ab/file.gz",
		"2009/03/ab/file.gz",
		"ndt/summary/2009/03/01/file.gz",
		"summary/2009/03/01/file.gz",
	} {
		if filename.Internal(badString).Lint() == nil {
			t.Errorf("Should have had a lint error on %q", badString)
		}
	}
	for _, goodString := range []string{
		"2009/03/13/file.gz",
		"2013/01/01/subdirectory/file.tgz",
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
