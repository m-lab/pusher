package main

import (
	"flag"
	"log"
	"math/rand"
	"time"

	"github.com/m-lab/pusher/bytecount"
	"github.com/m-lab/pusher/finder"
	"github.com/m-lab/pusher/listener"
	"github.com/m-lab/pusher/namer"
	"github.com/m-lab/pusher/tarcache"
	"github.com/m-lab/pusher/uploader"
	"github.com/m-lab/pusher/util"
)

var (
	project         = flag.String("project", "mlab-sandbox", "The google cloud project")
	directory       = flag.String("directory", "/var/spool/test", "The directory to watch for files.")
	bucket          = flag.String("bucket", "scraper-mlab-sandbox", "The GCS bucket to upload data to")
	experiment      = flag.String("experiment", "exp", "The name of the experiment generating the data")
	node            = flag.String("node", "mlab5", "The name of the node at the site")
	site            = flag.String("site", "lga0t", "The name of the mlab site for the node")
	ageThreshold    = flag.Duration("file_age_threshold", time.Duration(2)*time.Hour, "The maximum amount of time we should hold onto a piece of data before uploading it.")
	maxFileAge      = flag.Duration("max_file_age", time.Duration(4)*time.Hour, "If a file hasn't been modified in max_file_age, then it should be uploaded.  This is the 'cleanup' upload in case an event was missed.")
	cleanupInterval = flag.Duration("cleanup_interval", time.Duration(1)*time.Hour, "Run the cleanup job with this frequency.")
	sizeThreshold   = bytecount.ByteCount(20 * bytecount.Megabyte)
)

func init() {
	flag.Var(&sizeThreshold, "archive_size_threshold", "The minimum tarfile size we require to commence upload (1KB, 200MB, etc). Default is 20MB")
}

// TODO: Add prometheus metrics.

func main() {
	flag.Parse()
	util.ArgsFromEnv(flag.CommandLine)

	// Set up the upload system.
	namer := namer.New(*experiment, *node, *site)
	uploader, err := uploader.Create(*project, *bucket, namer)
	if err != nil {
		log.Fatal("Could not create uploader:", err)
	}
	tarCache, pusherChannel := tarcache.New(*directory, sizeThreshold, *ageThreshold, uploader)
	go tarCache.ListenForever()

	// Send all file close and file move events to the tarCache.
	l, err := listener.Create(*directory, pusherChannel)
	if err != nil {
		log.Fatal("Could not create the listener:", err)
	}
	defer l.Stop()
	go l.ListenForever()

	// Send very old files to the tarCache.
	//
	// We still need a find-based cleanup for two reasons:
	// 1. If closed files exist in the directory when the program starts, there is
	//    no way to know whether they are open or not. So, if they are older than the
	//    max_file_age, we will assume that the files are closed and upload them.
	//
	// 2. There is a race condition in the notify library where it is possible to
	//    create a directory and then create a file in the directory before the
	//    recursive listener has been established. We work around this bug (and any
	//    other bugs) by having a "cleanup" job that unconditionally adds any files
	//    older than the max_file_age.
	for {
		files, err := finder.FindFiles(*directory, *maxFileAge)
		log.Println(files, err)
		for _, file := range files {
			pusherChannel <- file
		}
		// Randomize the sleep time in an effort to avoid thundering herd problems
		// after container restarts. We're not worried about overloading GCS, but
		// without this we might end up running `find` for every experiment
		// simultaneously forever, and periodically run the disk out of IOPs. Using
		// ExpFloat64 in this way should ensure that the time between `find`
		// operations is mostly memoryless.
		time.Sleep(time.Duration(rand.ExpFloat64()*cleanupInterval.Seconds()) * time.Second)
	}
}
