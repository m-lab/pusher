package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/m-lab/go/prometheusx"

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
	monitorAddr     = flag.String("monitoring_address", ":9000", "The address and port on which prometheus metrics should be exported.")
	project         = flag.String("project", "mlab-sandbox", "The google cloud project")
	directory       = flag.String("directory", "/var/spool", "The directory containing one subdirectory per datatype.")
	bucket          = flag.String("bucket", "scraper-mlab-sandbox", "The GCS bucket to upload data to")
	experiment      = flag.String("experiment", "exp", "The name of the experiment generating the data")
	nodeName        = flag.String("mlab_node_name", "mlab5.abc0t.measurement-lab.org", "FQDN of the M-Lab node. Used to extract machine (mlab5) and site (abc0t) names.")
	ageThreshold    = flag.Duration("file_age_threshold", time.Duration(2)*time.Hour, "The maximum amount of time we should hold onto a piece of data before uploading it.")
	sizeThreshold   = bytecount.ByteCount(20 * bytecount.Megabyte)
	cleanupInterval = flag.Duration("cleanup_interval", time.Duration(1)*time.Hour, "Run the cleanup job with this frequency.")
	maxFileAge      = flag.Duration("max_file_age", time.Duration(4)*time.Hour, "If a file hasn't been modified in max_file_age, then it should be uploaded.  This is the 'cleanup' upload in case an event was missed.")
	dryRun          = flag.Bool("dry_run", false, "Start up the binary and then immmediately exit. Useful for verifying that the binary can actually run inside the container.")
	datatypes       = flagx.StringArray{}

	// Create a single unified context and a cancellationMethod for said context.
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

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(),
			`
Every flag can also be set by setting an all-caps environment variable with
the same name as the flag. For example if "-bucket" was not on the
commandline, the GCS bucket to use can also be set by the $BUCKET environment
variable.

All unparsed command-line arguments will be treated as independent datatypes
for uploading to GCS. These datatypes determine the subdirectory of GCS the
data is uploaded to, and they determine the subdirectory of /var/spool to
watch. Best practices dictate that these names should also be the first part
of the hostname of the machine, the name of the tables where this data
arrives in BigQuery, and consist only of the ASCII [0-9a-zA-Z] without
spaces, dashes, underscores, or any other special characters.
`)
	}
	// We want to get flag values from the environment or from the command-line.
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not parse flags from the environment")

	if len(datatypes) == 0 {
		logFatal("You must specify at least one datatype")
	}

	if *dryRun {
		cancelCtx()
	} else {
		defer cancelCtx()
	}

	for _, datatype := range datatypes {
		// Set up the upload system.
		namer, err := namer.New(datatype, *experiment, *nodeName)
		rtx.Must(err, "Could not create a new Namer")
		client, err := storage.NewClient(ctx)
		rtx.Must(err, "Could not create cloud storage client")

		uploader := uploader.Create(ctx, stiface.AdaptClient(client), *bucket, namer)

		datadir := filename.System(path.Join(*directory, datatype))

		// Set up the file-bundling tarcache system.
		tarCache, pusherChannel := tarcache.New(datadir, datatype, sizeThreshold, *ageThreshold, uploader)
		go tarCache.ListenForever(ctx)

		// Send all file close and file move events to the tarCache.
		l, err := listener.Create(datadir, pusherChannel)
		rtx.Must(err, "Could not create listener")
		go l.ListenForever(ctx)

		// Send very old or missed files to the tarCache as a cleanup precaution.
		go finder.FindForever(ctx, datadir, *maxFileAge, pusherChannel, *cleanupInterval)
	}

	// Start up the monitoring service.
	metricServer := prometheusx.MustStartPrometheus(*monitorAddr)
	<-ctx.Done()
	metricServer.Shutdown(ctx)
}
