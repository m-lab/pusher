package pusher

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestFindFiles(t *testing.T) {
	tempdir, _ := ioutil.TempDir("/tmp", "find_file_test")
	defer os.RemoveAll(tempdir)
	file1, _ := os.Create(tempdir + "/next_oldest_file")
	_, _ = file1.WriteString("data\n")
	_ = file1.Close()
	newtime := time.Now().Add(time.Duration(-12) * time.Hour)
	_ =os.Chtimes(tempdir + "/next_oldest_file", newtime, newtime)
	err, localfiles := FindFiles(tempdir, time.Duration(6) * time.Hour)
	if err != nil {
		t.Error(err)
	}
	if len(localfiles) != 1 {
		t.Errorf("len(localfiles) (%d) != 1", len(localfiles))
	}
	if localfiles[0].fullRelativeName != tempdir + "/next_oldest_file" {
		t.Errorf("wrong name: %s", localfiles[0].fullRelativeName)
	}
}
