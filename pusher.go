package main

import (
	"flag"
	"log"
	"time"

	"github.com/m-lab/pusher/bytecount"
	"github.com/m-lab/pusher/finder"
	"github.com/m-lab/pusher/namer"
	"github.com/m-lab/pusher/tarcache"
	"github.com/m-lab/pusher/uploader"
)

var (
	project       = flag.String("project", "mlab-sandbox", "The google cloud project")
	directory     = flag.String("directory", "/var/spool/test", "The directory to watch for files.")
	bucket        = flag.String("bucket", "scraper-mlab-sandbox", "The GCS bucket to upload data to")
	experiment    = flag.String("experiment", "exp", "The name of the experiment generating the data")
	node          = flag.String("node", "mlab5", "The name of the node at the site")
	site          = flag.String("site", "lga0t", "The name of the mlab site for the node")
	ageThreshold  = flag.Duration("file_age_threshold", time.Duration(2)*time.Hour, "The maximum amount of time we should hold onto a piece of data before uploading it.")
	sizeThreshold = bytecount.ByteCount(20 * bytecount.Megabyte)
)

func init() {
	flag.Var(&sizeThreshold, "archive_size_threshold", "The minimum tarfile size we require to commence upload (1KB, 200MB, etc). Default is 20MB")
}

// TODO: Add prometheus metrics.

func main() {
	flag.Parse()
	namer := namer.New(*experiment, *node, *site)
	uploader, err := uploader.New(*project, *bucket, namer)
	if err != nil {
		log.Fatal("Could not create uploader:", err)
	}
	tarCache, pusherChannel := tarcache.New(*directory, sizeThreshold, *ageThreshold, uploader)
	go tarCache.ListenForever()

	// TODO: only do this FindFiles thing on startup. Once everything is
	// started, use inotify listeners to get notified on file close events
	// in the directory and its subdirectories.
	interval := time.Duration(10) * time.Minute
	for {
		files, err := finder.FindFiles(*directory, interval)
		log.Println(files, err)
		for _, file := range files {
			pusherChannel <- file
		}
		time.Sleep(interval)
	}

}
