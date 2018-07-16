package uploader

import (
	"bytes"
	"encoding/base64"
	"math/rand"
	"os/exec"
	"testing"
	"time"
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
	uploader, err := New("mlab-testing", "archive-mlab-testing", namer)
	if err != nil {
		t.Fatal("Could not make uploader:", err)
	}
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
