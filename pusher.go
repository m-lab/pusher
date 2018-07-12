package main

import (
	"flag"
	"log"
	"time"

	"github.com/m-lab/pusher/bytecount"
	"github.com/m-lab/pusher/finder"
	"github.com/m-lab/pusher/tarcache"
	"github.com/m-lab/pusher/uploader"
)

var (
	directory     = flag.String("directory", "/var/spool/test", "The directory to watch for files.")
	project       = flag.String("project", "mlab-sandbox", "The GCP project for uploading")
	bucket        = flag.String("bucket", "dropbox-mlab-sandbox", "The GCP bucket for uploading")
	ageThreshold  = flag.Duration("file_age_threshold", time.Duration(2)*time.Hour, "The maximum amount of time we should hold onto a piece of data before uploading it.")
	sizeThreshold = bytecount.ByteCount(20 * bytecount.Megabyte)
)

func init() {
	flag.Var(&sizeThreshold, "archive_size_threshold", "The minimum tarfile size we require to commence upload (1KB, 200MB, etc). Default is 20MB")
}

func main() {
	flag.Parse()
	uploader := uploader.New(*project, *bucket)
	tarCache, pusherChannel := tarcache.New(*directory, sizeThreshold, *ageThreshold, uploader)
	go tarCache.ListenForever()

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
