package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/go/bytecount"
	flagx "github.com/m-lab/go/flagext"
	r "github.com/m-lab/go/runtimeext"

	"github.com/m-lab/pusher/finder"
	"github.com/m-lab/pusher/listener"
	"github.com/m-lab/pusher/namer"
	"github.com/m-lab/pusher/tarcache"
	"github.com/m-lab/pusher/uploader"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	monitorAddr     = flag.String("monitoring_address", ":9000", "The address and port on which prometheus metrics should be exported.")
	project         = flag.String("project", "mlab-sandbox", "The google cloud project")
	directory       = flag.String("directory", "/var/spool/test", "The directory to watch for files.")
	bucket          = flag.String("bucket", "scraper-mlab-sandbox", "The GCS bucket to upload data to")
	experiment      = flag.String("experiment", "exp", "The name of the experiment generating the data")
	nodeName        = flag.String("mlab_node_name", "mlab5.abc0t.measurement-lab.org", "FQDN of the M-Lab node. Used to extract machine (mlab5) and site (abc0t) names.")
	ageThreshold    = flag.Duration("file_age_threshold", time.Duration(2)*time.Hour, "The maximum amount of time we should hold onto a piece of data before uploading it.")
	sizeThreshold   = bytecount.ByteCount(20 * bytecount.Megabyte)
	cleanupInterval = flag.Duration("cleanup_interval", time.Duration(1)*time.Hour, "Run the cleanup job with this frequency.")
	maxFileAge      = flag.Duration("max_file_age", time.Duration(4)*time.Hour, "If a file hasn't been modified in max_file_age, then it should be uploaded.  This is the 'cleanup' upload in case an event was missed.")
)

func init() {
	flag.Var(&sizeThreshold, "archive_size_threshold", "The minimum tarfile size we require to commence upload (1KB, 200MB, etc). Default is 20MB")
}

func main() {
	// We want to get flag values from the environment or from the command-line.
	flag.Parse()
	flagx.ArgsFromEnv(flag.CommandLine)

	// Create a context that will allow cancellation of all the component goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up the upload system.
	namer, err := namer.New(*experiment, *nodeName)
	r.Must(err, "Could not create a new Namer")
	client, err := storage.NewClient(ctx)
	r.Must(err, "Could not create cloud storage client")

	uploader := uploader.Create(ctx, stiface.AdaptClient(client), *bucket, namer)

	// Set up the file-bundling tarcache system.
	tarCache, pusherChannel := tarcache.New(*directory, sizeThreshold, *ageThreshold, uploader)
	go tarCache.ListenForever(ctx)

	// Send all file close and file move events to the tarCache.
	l, err := listener.Create(*directory, pusherChannel)
	r.Must(err, "Could not create listener")
	go l.ListenForever(ctx)

	// Send very old or missed files to the tarCache as a cleanup precaution.
	go finder.FindForever(ctx, *directory, *maxFileAge, pusherChannel, *cleanupInterval)

	// Start up the monitoring service.
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*monitorAddr, nil))
}
