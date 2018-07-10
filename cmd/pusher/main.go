package main

import (
	"flag"
	"github.com/m-lab/pusher"
	"log"
	"math"
	"math/rand"
	"time"
)

var (
	directory             = flag.String("directory", "/var/spool/ndt_iupui", "The directory containing the files to upload.")
	upload_server         = flag.String("upload_server", "https://uploader-mlab-oti.appspot.com", "What server to use to get the upload URL")
	data_buffer_threshold = flag.Int("data_buffer_threshold", 30000000, "The number of bytes to buffer before uploading")
	min_file_age          = flag.Duration("min_file_age", time.Duration(2)*time.Hour, "The amount of time that must have elapsed since the last edit of a file must be before it is eligible for upload")
)

func main() {
	flag.Parse()
	channel := pusher.StartUploader(*upload_server, int64(*data_buffer_threshold))
	for {
		err, files := pusher.FindFiles(*directory, *min_file_age)
		if err != nil {
			log.Printf("Could not find files in %s (%s)", *directory, err)
		} else {
			for _, file := range files {
				channel <- file
			}
		}
		sleepTime := math.Min(rand.ExpFloat64() * 60, 300)
		time.Sleep(time.Duration(sleepTime) * time.Second)
	}
}
