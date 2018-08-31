package uploader_test

import (
	"bytes"
	"encoding/base64"
	"math/rand"
	"os/exec"
	"testing"
	"time"

	"github.com/m-lab/pusher/uploader"
)

type testNamer struct {
	newName string
}

func (tn testNamer) ObjectName(t time.Time) string {
	return tn.newName
}

func TestUploading(t *testing.T) {
	buff := make([]byte, 16)
	rand.Seed(time.Now().UnixNano())
	rand.Read(buff)
	fileName := "TestUploading." + base64.RawURLEncoding.EncodeToString(buff)
	namer := &testNamer{
		newName: fileName,
	}
	uploader := uploader.MustCreate("mlab-testing", "archive-mlab-testing", namer)
	buffer := new(bytes.Buffer)
	contents := "contentofatarfile"
	buffer.WriteString(contents)
	if err := uploader.Upload(buffer); err != nil {
		t.Error("Could not Upload():", err)
	}
	url := "https://storage.googleapis.com/archive-mlab-testing/" + fileName
	defer func() {
		cmd := exec.Command("gsutil", "rm", "-f", "gs://archive-mlab-testing/"+fileName)
		cmd.Run()
	}()

	getter := exec.Command("curl", url)
	var out bytes.Buffer
	getter.Stdout = &out
	if err := getter.Run(); err != nil {
		t.Errorf("curl command failed: %q", err)
	}
	if s := string(out.Bytes()); s != contents {
		t.Errorf("File contents %q != %q (url: %q)", s, contents, url)
	}
}
