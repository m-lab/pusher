package uploader_test

import (
	"bytes"
	"encoding/base64"
	"math/rand"
	"os/exec"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/pusher/filename"
	"github.com/m-lab/pusher/uploader"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
)

type testNamer struct {
	newName string
}

func (tn testNamer) ObjectName(s filename.System, t time.Time) string {
	return tn.newName
}

func TestUploading(t *testing.T) {
	buff := make([]byte, 16)
	rand.Seed(time.Now().UnixNano())
	rand.Read(buff)
	dir := filename.System("TestUploading." + base64.RawURLEncoding.EncodeToString(buff) + "/")
	fileName := dir + "test.txt"
	namer := &testNamer{
		newName: string(fileName),
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Error("Could not create storage client:", err)
	}
	up := uploader.Create(ctx, stiface.AdaptClient(client), "archive-mlab-testing", namer)
	contents := "contentofatarfile"
	if err := up.Upload(dir, []byte(contents)); err != nil {
		t.Error("Could not Upload():", err)
	}
	url := "https://storage.googleapis.com/archive-mlab-testing/" + string(fileName)
	defer func() {
		cmd := exec.Command("gsutil", "rm", "-f", "gs://archive-mlab-testing/"+string(fileName))
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
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Error("Could not create storage client:", err)
	}
	up := uploader.Create(ctx, stiface.AdaptClient(client), "archive-mlab-testing", namer)
	err = up.Upload("test/", []byte("contents"))
	if err == nil {
		t.Error("Should not have been able to Upload() badfilename")
	}
}

// Adapted from google-cloud-go-testing/storage/stiface/stiface_test.go
//
// By using the "interface" version of the client, we make it possible to sub in
// our own fakes at any level. Here we sub in a fake Client which returns a fake
// BucketHandle that returns a fake Object, that returns a Writer in which all
// writes will fail. This is only a "blackbox" test in the most technical of
// senses, but it allows us to exercise error paths.
type fakeClient struct {
	stiface.Client
}

func (f fakeClient) Bucket(name string) stiface.BucketHandle {
	return &fakeBucketHandle{}
}

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
	calls int
}

// The first three writes succeed and each writes one byte to this slice.
var firstThreeBytes = make([]byte, 3)

// The first three calls succeed and write a single byte, and then it fails forever.
func (f *failingWriter) Write(p []byte) (n int, err error) {
	if f.calls < 3 {
		firstThreeBytes[f.calls] = p[0]
		f.calls++
		return 1, nil
	} else {
		err := &googleapi.Error{}
		return 0, err
	}
}

func (f fakeErroringObjectHandle) NewWriter(ctx context.Context) stiface.Writer {
	return &failingWriter{}
}

// A test to execute error paths.
func TestUploadFailure(t *testing.T) {
	up := uploader.Create(context.Background(), &fakeClient{}, "archive-mlab-testing", &testNamer{"OkayFilename"})
	err := up.Upload("test/", []byte("contents"))
	if err == nil {
		t.Error("Should not have been able to Upload() the writer that fails.")
	}
	if string(firstThreeBytes) != "con" {
		t.Error("The contents of the string were not partially written correctly")
	}
}
