package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/memoryless"
	"github.com/m-lab/go/osx"
	"github.com/m-lab/go/prometheusx/promtest"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/pusher/filename"
	"github.com/m-lab/pusher/listener"
	"github.com/m-lab/pusher/tarcache"
	"github.com/m-lab/pusher/uploader"

	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
)

func TestMainDoesCrashOnEmptyDatatypes(t *testing.T) {
	fatalCalled := false
	logFatal = func(i ...interface{}) {
		fatalCalled = true
		panic(i)
	}
	defer func() {
		logFatal = log.Fatal
	}()
	defer func() {
		recover()
		if !fatalCalled {
			t.Error("Fatal was never called")
		}
	}()

	datatypes = []string{}
	main()
}

func TestMainDoesntCrash(t *testing.T) {
	ctx, cancelCtx = context.WithCancel(context.Background())
	tempdir, err := ioutil.TempDir("/tmp", "pusher_main_test.TestMain")
	defer os.RemoveAll(tempdir)
	if err != nil {
		t.Error(err)
		return
	}
	rtx.Must(os.Mkdir(tempdir+"/testdata", 0777), "Could not create dir.")
	// Set up the environment variables.
	type TempEnvVar struct {
		name, value string
	}
	newVars := []TempEnvVar{
		{"PROJECT", "mlab-testing"},
		{"DIRECTORY", tempdir},
		{"BUCKET", "archive-mlab-testing"},
		{"EXPERIMENT", "exp"},
		{"MLAB_NODE_NAME", "mlab5.abc1t.measurement-lab.org"},
		{"MONITORING_ADDRESS", "localhost:9000"},
		{"DATATYPE", "testdata"},
	}
	for i := range newVars {
		revert := osx.MustSetenv(newVars[i].name, newVars[i].value)
		defer revert()
	}
	go func() {
		// Wait 2 seconds to lose all race conditions.
		time.Sleep(2 * time.Second)
		cancelCtx()
	}()
	main()
	// When this exits, we're good.

	// Make sure our custom usage message doesn't crash everything.
	flag.Usage()

	// As an extra test, double-check that with DRY_RUN set to true, main() exits right away.
	ctx, cancelCtx = context.WithCancel(context.Background())
	defer cancelCtx() // Prevent the linter from complaining about the leak of cancelCtx
	revert := osx.MustSetenv("DRY_RUN", "true")
	defer revert()
	main()
}

func TestLintMetrics(t *testing.T) {
	promtest.LintMetrics(t)
}

type fakeNamer struct {
	name string
}

func (f fakeNamer) ObjectName(_ filename.System, _ time.Time) string {
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

	tarCache, pusherChannel := tarcache.New(filename.System(tempdir), "test", &flagx.KeyValue{}, 1, memoryless.Config{}, up)
	go tarCache.ListenForever(ctx, ctx)

	// Set up the listener on the temp directory.
	l, err := listener.Create(filename.System(tempdir), pusherChannel)
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
	cloudContents, err := ioutil.ReadFile(tardir + "/tinyfile")
	if err != nil {
		t.Errorf("Could not read %s (%v)", tardir+"/tinyfile", err)
	}
	if string(cloudContents) != contents {
		t.Errorf("File contents %q != %q (url: %q)", string(cloudContents), contents, url)
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
	}
	log.Println("Creating a new error object")
	s.objectcount++
	return fakeErroringObjectHandle{}
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

	tarCache, pusherChannel := tarcache.New(filename.System(tempdir), "testdata", &flagx.KeyValue{}, 1, memoryless.Config{}, up)
	go tarCache.ListenForever(ctx, ctx)

	// Set up the listener on the temp directory.
	l, err := listener.Create(filename.System(tempdir), pusherChannel)
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
	cloudContents, err := ioutil.ReadFile(tardir + "/tinyfile")
	if err != nil {
		t.Errorf("Could not read %s (%v)", tardir+"/tinyfile", err)
	}
	if string(cloudContents) != contents {
		t.Errorf("File contents %q != %q (url: %q)", string(cloudContents), contents, url)
	}
}

func TestSignalHandler(t *testing.T) {
	ctx, cancelCtx = context.WithCancel(context.Background())
	defer cancelCtx()
	wg := sync.WaitGroup{}
	wg.Add(2)
	waitTime := time.Duration(100 * time.Millisecond)
	var cancel1Time, cancel2Time time.Time
	var canceled1, canceled2 bool
	mu := sync.Mutex{}
	cancel1 := func() {
		mu.Lock()
		defer mu.Unlock()
		canceled1 = true
		cancel1Time = time.Now()
		wg.Done()
	}
	cancel2 := func() {
		mu.Lock()
		defer mu.Unlock()
		canceled2 = true
		cancel2Time = time.Now()
		wg.Done()
	}
	go signalHandler(syscall.SIGUSR2, cancel1, waitTime, cancel2)
	time.Sleep(100 * time.Millisecond) // Give the signal handler time to set up

	// Verify that nothing is yet canceled.
	mu.Lock()
	if canceled1 || canceled2 {
		t.Error("Nothing should be canceled yet", canceled1, canceled2)
	}
	mu.Unlock()

	// Send the signal
	p, err := os.FindProcess(os.Getpid())
	rtx.Must(err, "Could not get the current process")
	p.Signal(syscall.SIGUSR2)

	// Verify that cancel, wait, cancel is what happened.
	wg.Wait()
	timeBetweenCancels := cancel2Time.Sub(cancel1Time)
	if timeBetweenCancels < waitTime/2 || timeBetweenCancels > waitTime*2 {
		t.Errorf("%v is nowhere near %v", timeBetweenCancels, waitTime)
	}
}
