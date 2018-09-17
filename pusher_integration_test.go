package main_test

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	rtx "github.com/m-lab/go/runtimeext"
	"github.com/m-lab/pusher/listener"
	"github.com/m-lab/pusher/tarcache"
	"github.com/m-lab/pusher/uploader"

	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
)

type fakeNamer struct {
	name string
}

func (f fakeNamer) ObjectName(t time.Time) string {
	log.Println("Returned object name:", f.name)
	return f.name
}

// Set up the three main components and verify that they all work together correctly.
func TestListenerTarcacheAndUploader(t *testing.T) {
	// Set up the Uploader to create an error and then work
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := storage.NewClient(ctx)
	rtx.Must(err, "Could not create cloud storage client")
	namer := &fakeNamer{fmt.Sprintf("TestListenerTarcacheAndUploader-%d", time.Now().Unix())}
	up := uploader.Create(ctx, stiface.AdaptClient(client), "archive-mlab-testing", namer)

	// Set up the TarCache with the uploader
	tempdir, err := ioutil.TempDir("/tmp", "pusher_main_test.TestListenerTarcacheAndUploader")
	defer os.RemoveAll(tempdir)
	if err != nil {
		t.Error(err)
		return
	}

	tarCache, pusherChannel := tarcache.New(tempdir, 1, 1, up)
	go tarCache.ListenForever(ctx)

	// Set up the listener on the temp directory.
	l, err := listener.Create(tempdir, pusherChannel)
	rtx.Must(err, "Could not create listener")
	go l.ListenForever(ctx)

	// Lose the race condition with the setup of the inotify listener.
	time.Sleep(1 * time.Second)

	// Send enough data to the TarCache to cause an upload
	contents := "abcdefghijklmnop"
	ioutil.WriteFile(tempdir+"/tinyfile", []byte(contents), os.FileMode(0666))

	// Lose the race condition with the inotify listener and the upload.
	time.Sleep(1 * time.Second)

	// Verify that the data from the TarCache was successfully uploaded
	url := "https://storage.googleapis.com/archive-mlab-testing/" + namer.name
	defer func() {
		cmd := exec.Command("gsutil", "rm", "-f", "gs://archive-mlab-testing/"+namer.name)
		cmd.Run()
	}()

	// Make a place to put the tarfile.
	tardir, err := ioutil.TempDir("/tmp", "pusher_main_test.TestListenerTarcacheAndUploader.tarfiledir")
	defer os.RemoveAll(tardir)
	if err != nil {
		t.Error(err)
		return
	}

	// Download the tarfile and untar it.
	getter := exec.Command("curl", url, "--output", tardir+"/tarfile.tgz")
	if err := getter.Run(); err != nil {
		t.Errorf("curl command failed: %q", err)
	}
	untarrer := exec.Command("tar", "xfz", tardir+"/tarfile.tgz", "-C", tardir)
	if err := untarrer.Run(); err != nil {
		t.Errorf("tar command failed: %q", err)
	}

	// Verify that the contents of "tinyfile" from the tarfile match what we wrote the FS in the first place.
	cloud_contents, err := ioutil.ReadFile(tardir + "/tinyfile")
	if err != nil {
		t.Errorf("Could not read %s (%v)", tardir+"/tinyfile", err)
	}
	if string(cloud_contents) != contents {
		t.Errorf("File contents %q != %q (url: %q)", string(cloud_contents), contents, url)
	}
}

// A GCS client that will have one error and then will work. This verifies that
// that whole system works even when connectivity to GCS is flaky.
type singleErrorClient struct {
	stiface.Client
	realClient stiface.Client
}

func (s singleErrorClient) Bucket(name string) stiface.BucketHandle {
	return &singleErrorBucketHandle{
		realBucketHandle: s.realClient.Bucket(name),
	}
}

type singleErrorBucketHandle struct {
	stiface.BucketHandle
	realBucketHandle stiface.BucketHandle
	objectcount      int
}

func (s *singleErrorBucketHandle) Object(name string) stiface.ObjectHandle {
	if s.objectcount > 0 {
		return s.realBucketHandle.Object(name)
	} else {
		log.Println("Creating a new error object")
		s.objectcount++
		return fakeErroringObjectHandle{}
	}
}

type fakeErroringObjectHandle struct {
	stiface.ObjectHandle
}

func (f fakeErroringObjectHandle) NewWriter(ctx context.Context) stiface.Writer {
	return &failingWriter{}
}

type failingWriter struct {
	stiface.Writer
}

func (f failingWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("This should fail immediately")
}

func TestListenerTarcacheAndUploaderWithOneFailure(t *testing.T) {
	// Set up the Uploader to create an error and then work
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := storage.NewClient(ctx)
	rtx.Must(err, "Could not create cloud storage client")
	namer := &fakeNamer{fmt.Sprintf("TestListenerTarcacheAndUploaderWithOneFailure-%d", time.Now().Unix())}
	up := uploader.Create(ctx, singleErrorClient{realClient: stiface.AdaptClient(client)}, "archive-mlab-testing", namer)

	// Set up the TarCache with the uploader
	tempdir, err := ioutil.TempDir("/tmp", "pusher_main_test.TestListenerAndUploaderWithOneFailure")
	defer os.RemoveAll(tempdir)
	if err != nil {
		t.Error(err)
		return
	}

	tarCache, pusherChannel := tarcache.New(tempdir, 1, 1, up)
	go tarCache.ListenForever(ctx)

	// Set up the listener on the temp directory.
	l, err := listener.Create(tempdir, pusherChannel)
	rtx.Must(err, "Could not create listener")
	go l.ListenForever(ctx)

	// Lose the race condition with the setup of the inotify listener.
	time.Sleep(1 * time.Second)

	// Send enough data to the TarCache to cause an upload
	contents := "abcdefghijklmnop"
	ioutil.WriteFile(tempdir+"/tinyfile", []byte(contents), os.FileMode(0666))

	// Lose the race condition with the inotify listener and the upload.
	time.Sleep(1 * time.Second)

	// Verify that the data from the TarCache was successfully uploaded
	url := "https://storage.googleapis.com/archive-mlab-testing/" + namer.name
	defer func() {
		cmd := exec.Command("gsutil", "rm", "-f", "gs://archive-mlab-testing/"+namer.name)
		cmd.Run()
	}()

	// Make a place to put the tarfile.
	tardir, err := ioutil.TempDir("/tmp", "pusher_main_test.TestListenerTarcacheAndUploadWithOneFailure.tarfiledir")
	defer os.RemoveAll(tardir)
	if err != nil {
		t.Error(err)
		return
	}

	// Download the tarfile and untar it.
	getter := exec.Command("curl", url, "--output", tardir+"/tarfile.tgz")
	if err := getter.Run(); err != nil {
		t.Errorf("curl command failed: %q", err)
	}
	untarrer := exec.Command("tar", "xfz", tardir+"/tarfile.tgz", "-C", tardir)
	if err := untarrer.Run(); err != nil {
		t.Errorf("tar command failed: %q", err)
	}

	// Verify that the contents of "tinyfile" from the tarfile match what we wrote the FS in the first place.
	cloud_contents, err := ioutil.ReadFile(tardir + "/tinyfile")
	if err != nil {
		t.Errorf("Could not read %s (%v)", tardir+"/tinyfile", err)
	}
	if string(cloud_contents) != contents {
		t.Errorf("File contents %q != %q (url: %q)", string(cloud_contents), contents, url)
	}
}
