package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/m-lab/go/prometheusx"
	"github.com/m-lab/go/uniformnames"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/go/bytecount"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/pusher/filename"
	"github.com/m-lab/pusher/finder"
	"github.com/m-lab/pusher/listener"
	"github.com/m-lab/pusher/namer"
	"github.com/m-lab/pusher/tarcache"
	"github.com/m-lab/pusher/uploader"
)

var (
	project         = flag.String("project", "mlab-sandbox", "The google cloud project")
	directory       = flag.String("directory", "/var/spool", "The directory containing one subdirectory per datatype.")
	bucket          = flag.String("bucket", "pusher-mlab-sandbox", "The GCS bucket to upload data to")
	experiment      = flag.String("experiment", "exp", "The name of the experiment generating the data")
	nodeName        = flag.String("mlab_node_name", "mlab5.abc0t.measurement-lab.org", "FQDN of the M-Lab node. Used to extract machine (mlab5) and site (abc0t) names.")
	ageThreshold    = flag.Duration("file_age_threshold", time.Duration(2)*time.Hour, "The maximum amount of time we should hold onto a piece of data before uploading it.")
	sizeThreshold   = bytecount.ByteCount(20 * bytecount.Megabyte)
	cleanupInterval = flag.Duration("cleanup_interval", time.Duration(1)*time.Hour, "Run the cleanup job with this frequency.")
	maxFileAge      = flag.Duration("max_file_age", time.Duration(4)*time.Hour, "If a file hasn't been modified in max_file_age, then it should be uploaded.  This is the 'cleanup' upload in case an event was missed.")
	dryRun          = flag.Bool("dry_run", false, "Start up the binary and then immmediately exit. Useful for verifying that the binary can actually run inside the container.")
	datatypes       = flagx.StringArray{}
	sigtermWait     = flag.Duration("sigterm_wait_time", time.Duration(150*time.Second), "How long to wait after receving a SIGTERM before we upload everything on an emergenecy basis.")

	// Create a single unified context and a cancellation method for said context.
	ctx, cancelCtx = context.WithCancel(context.Background())

	// A shim for log.Fatal to allow mocking for testing.
	logFatal = log.Fatal
)

func init() {
	// Set up the size flag with a custom parser.
	flag.Var(&sizeThreshold, "archive_size_threshold", "The minimum tarfile size we require to commence upload (1KB, 200MB, etc). Default is 20MB")
	// Set up the datatype flag with the appropriate parser.
	flag.Var(&datatypes, "datatype", "The datatype to scrape within the directory. This argument should appear at least once, and may appear multiple times.")
}

// signalHandler allows the pusher to upload as much data as possible after a
// sigterm is received. TarCache contains two contexts. One that causes data to
// be uploaded immediately, but allows data collection to continue, and one that
// causes data to be uploaded and the the tarcache loop exits.
//
// Receving a signal in this signal handler sets pusher on an inexorable path
// for main to exit.
//
// The signal handler, when this process receives the appropriate signal from
// the OS, cancels the first context, waits for a bit, and then cancels the
// second context. In this way, we ensure that as much data as possible has been
// successfully uploaded when pusher exits.
func signalHandler(sig os.Signal, termCancel context.CancelFunc, waitTime time.Duration, killCancel context.CancelFunc) {
	// Set up the signal handler.
	c := make(chan os.Signal, 1)
	signal.Notify(c, sig)

	// Wait until we get a signal or the overall context is canceled.
	select {
	case <-c:
		log.Println("Signal received")
	case <-ctx.Done():
		log.Println("Context canceled")
	}

	// Start the timer before we do anything else, to ensure that timer time and
	// wall clock time are as aligned as possible.
	timer := time.NewTimer(waitTime)

	log.Printf("Signal received. Forcing emergency upload twice.")
	termCancel()
	log.Printf("First emergency upload complete. About to wait for %v.\n", waitTime)

	// Sleep, but stop sleeping if the context is canceled.
	select {
	case <-timer.C:
		log.Println("Timer complete")
	case <-ctx.Done():
		log.Println("Context canceled")
		timer.Stop()
	}

	log.Println("Beginning last emergency upload.")
	killCancel()
	log.Println("Last emergency upload complete.")
	cancelCtx()
	log.Println("Signal handler complete.")
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(),
			`
Every flag can also be set by setting an all-caps environment variable with
the same name as the flag. For example if "-bucket" was not on the
commandline, the GCS bucket to use can also be set by the $BUCKET environment
variable. The name of the experiment and datatypes should conform to the
M-Lab uniform naming conventions.
`)
	}
	// We want to get flag values from the environment or from the command-line.
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not parse flags from the environment")
	rtx.Must(uniformnames.Check(*experiment), "Experiment name %q did not conform to the unified naming convention", *experiment)
	for _, d := range datatypes {
		rtx.Must(uniformnames.Check(d), "Datatype name %d did not conform to the unified naming convention", d)
	}

	if len(datatypes) == 0 {
		logFatal("You must specify at least one datatype")
	}

	killContext, killCancel := context.WithCancel(ctx)
	defer killCancel()
	termContext, termCancel := context.WithCancel(killContext)
	defer termCancel()

	go signalHandler(syscall.SIGTERM, termCancel, *sigtermWait, killCancel)

	if *dryRun {
		cancelCtx()
	} else {
		defer cancelCtx()
	}

	// Start up the monitoring service.
	metricServer := prometheusx.MustServeMetrics()
	defer metricServer.Shutdown(ctx)

	// A waitgroup to allow us to keep the program running as long as tarcache
	// ListenForever loops are still running.
	wg := sync.WaitGroup{}

	// Set up pushing for every datatype.
	for _, datatype := range datatypes {
		// Set up the upload system.
		namer, err := namer.New(datatype, *experiment, *nodeName)
		rtx.Must(err, "Could not create a new Namer")
		client, err := storage.NewClient(ctx)
		rtx.Must(err, "Could not create cloud storage client")

		uploader := uploader.Create(ctx, stiface.AdaptClient(client), *bucket, namer)

		datadir := filename.System(path.Join(*directory, datatype))

		// Set up the file-bundling tarcache system.
		tc, pusherChannel := tarcache.New(datadir, datatype, sizeThreshold, *ageThreshold, uploader)
		wg.Add(1)
		go func() {
			tc.ListenForever(termContext, killContext)
			wg.Done()
		}()

		// Send all file close and file move events to the tarCache.
		l, err := listener.Create(datadir, pusherChannel)
		rtx.Must(err, "Could not create listener")
		go l.ListenForever(ctx)

		// Send very old or missed files to the tarCache as a cleanup precaution.
		go finder.FindForever(ctx, datadir, *maxFileAge, pusherChannel, *cleanupInterval)
	}

	// Wait until every TarCache.ListenForever loop has terminated. Once every loop
	// has terminated, pusher's reason to exist has disappeared too, so exit after.
	wg.Wait()
}
