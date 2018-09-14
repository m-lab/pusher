package uploader

import (
	"bytes"
	"encoding/base64"
	"errors"
	"math/rand"
	"os/exec"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"golang.org/x/net/context"
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
	up := MustCreate("mlab-testing", "archive-mlab-testing", namer)
	buffer := new(bytes.Buffer)
	contents := "contentofatarfile"
	buffer.WriteString(contents)
	if err := up.Upload(buffer); err != nil {
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

func TestUploadBadFilename(t *testing.T) {
	namer := &testNamer{"Bad\nFilename"}
	up := MustCreate("mlab-testing", "archive-mlab-testing", namer)
	buffer := new(bytes.Buffer)
	contents := "contents"
	buffer.WriteString(contents)
	err := up.Upload(buffer)
	if err == nil {
		t.Error("Should not have been able to Upload() badfilename")
	}
}

// Adapted from google-cloud-go-testing/storage/stiface/stiface_test.go
//
// By using the "interface" version of the client, we make it possible to sub in
// our own fakes at any level. Here we sub in a fake BucketHandle that returns a
// fake Object, that returns a Writer in which all writes will fail.
type fakeBucketHandle struct {
	stiface.BucketHandle
}

type fakeErroringObjectHandle struct {
	stiface.ObjectHandle
}

func (f fakeBucketHandle) Object(name string) stiface.ObjectHandle {
	return fakeErroringObjectHandle{}
}

type failingWriter struct {
	stiface.Writer
}

func (f failingWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("This should fail immediately")
}

func (f fakeErroringObjectHandle) NewWriter(ctx context.Context) stiface.Writer {
	return &failingWriter{}
}

// A whitebox test to execute error paths.
func TestUploadFailure(t *testing.T) {
	up := &uploader{
		context: context.Background(),
		namer:   &testNamer{"OkayFilename"},
		bucket:  &fakeBucketHandle{},
	}
	buffer := new(bytes.Buffer)
	contents := "contents"
	buffer.WriteString(contents)
	err := up.Upload(buffer)
	if err == nil {
		t.Error("Should not have been able to Upload() badfilename")
	}
}
