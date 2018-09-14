// Package uploader provides a tool for saving byte buffers to Google Cloud Storage.
package uploader

import (
	"bytes"
	"fmt"
	"time"

	"github.com/GoogleCloudPlatform/google-cloud-go-testing/storage/stiface"
	"github.com/m-lab/pusher/namer"
	"golang.org/x/net/context"
)

// Uploader is an interface for uploading data.
type Uploader interface {
	Upload(*bytes.Buffer) error
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
func (u *uploader) Upload(tarBuffer *bytes.Buffer) error {
	name := u.namer.ObjectName(time.Now().UTC())
	object := u.bucket.Object(name)
	writer := object.NewWriter(u.context)
	_, err := tarBuffer.WriteTo(writer)
	if err != nil {
		return fmt.Errorf("Could not write to gs://%s/%s (%v)", u.bucketName, name, err)
	}
	return writer.Close()
}
