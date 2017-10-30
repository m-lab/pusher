package pusher_test

import (
	"github.com/m-lab/pusher"
	"log"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
)

func TestUpload(t *testing.T) {
	t.Skip("Uploading is not yet implemented")
}

func TestDelete(t *testing.T) {
	t.Skip("Deleting is not yet implemented")
}

func TestCreateTarfile(t *testing.T) {
	tempdir, err := ioutil.TempDir("/tmp", "create_tarfile_test")
	if err != nil {
		t.Errorf("Bad tempdir: %s", err)
	}
	currdir, err := os.Getwd()
	if err != nil {
		t.Errorf("Bad Getcwd: %s", err)
	}
	err = os.Chdir(tempdir)
	if err != nil {
		t.Errorf("Bad Chdir: %s", err)
	}
	log.Printf("directory is: %s", tempdir)
	defer os.Chdir(currdir)
	defer os.RemoveAll(tempdir)
	file1, _ := os.Create("file1.txt")
	_, _ = file1.WriteString("twelve bytes")
	_ = file1.Sync()
	f1info, _ := file1.Stat()
	file2, _ := os.Create("file2.txt")
	_, _ = file2.WriteString("fourteen bytes")
	_ = file2.Sync()
	f2info, _ := file2.Stat()
	fileList := make([]*pusher.LocalDataFile, 2)
	fileList[0] = &pusher.LocalDataFile{
		FullRelativeName: "file1.txt",
		Info: f1info,
		CachedSize: 12,
	}
	fileList[1] = &pusher.LocalDataFile{
		FullRelativeName: "file2.txt",
		Info: f2info,
		CachedSize: 14,
	}
	err, buffer := pusher.CreateTarfile(fileList)
	if err != nil {
		t.Fatalf("Error: %s", err)
	}
	ioutil.WriteFile("files.tgz", buffer.Bytes(), 0666)
	out, err := exec.Command("tar", "tfz", "files.tgz").Output()
	if err != nil {
		t.Errorf("Bad command: %s", err)
	}
	if string(out) != "file1.txt\nfile2.txt\n" {
		t.Errorf("Bad output: %s", out)
	}
}

func TestBufferThenCall(t *testing.T) {
	t.Skip("Testing of BufferThenCall is not yet implemented")
}
