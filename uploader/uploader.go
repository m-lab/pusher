// Package uploader provides a tool for saving byte buffers to Google Cloud Storage.
package uploader

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/pusher/filename"
	"github.com/m-lab/pusher/namer"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
)

// Uploader is an interface for uploading data.
type Uploader interface {
	Upload(dir filename.System, contents []byte) error
}

// We split the Uploader into a struct and Interface to allow for mocking of the
// returned Uploader.
//
// Similarly, we use the stiface interface versions of Client and BucketHandle
// instead of raw pointers to allow for mocking of the Google Cloud Storage
// interface to aid in whitebox testing.
type uploader struct {
	context    context.Context
	namer      namer.Namer
	client     stiface.Client
	bucket     stiface.BucketHandle
	bucketName string
}

// Create and return a new object that implements Uploader.
func Create(ctx context.Context, client stiface.Client, bucketName string, namer namer.Namer) Uploader {
	// TODO: add timeouts and error handling to this.
	bucketHandle := client.Bucket(bucketName)
	return &uploader{
		context:    ctx,
		namer:      namer,
		client:     client,
		bucket:     bucketHandle,
		bucketName: bucketName,
	}
}

// Upload the provided buffer to GCS.
func (u *uploader) Upload(directory filename.System, contents []byte) error {
	name := u.namer.ObjectName(directory, time.Now().UTC())
	object := u.bucket.Object(name)
	writer := object.NewWriter(u.context)
	n, err := writer.Write(contents)
	for n != len(contents) || err != nil {
		if err != nil {
			msg := fmt.Sprintf("Could not write to gs://%s/%s (%v)", u.bucketName, name, err)
			if e, ok := err.(*googleapi.Error); ok {
				if e.Code == http.StatusTooManyRequests || e.Code >= http.StatusInternalServerError {
					// NOTE: may be verbose.
					msg += fmt.Sprintf(" googleapi.Error(%#v)", e)
				}
			}
			return errors.New(msg)
		}
		var newWrite int
		newWrite, err = writer.Write(contents[n:])
		n += newWrite
	}
	return writer.Close()
}
