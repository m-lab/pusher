// Package uploader provides a tool for saving byte buffers to Google Cloud Storage.
package uploader

import (
	"bytes"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	r "github.com/m-lab/go/runtimeext"
	"github.com/m-lab/pusher/namer"
	"golang.org/x/net/context"
)

// Uploader is an interface for uploading data.
type Uploader interface {
	Upload(*bytes.Buffer) error
}

// We split the Uploader into a struct and Interface to allow for mocking.
type uploader struct {
	context    context.Context
	namer      namer.Namer
	client     *storage.Client
	bucket     *storage.BucketHandle
	bucketName string
}

// MustCreate creates and return a new object that implements Uploader, or dies.
func MustCreate(project string, bucket string, namer namer.Namer) Uploader {
	// TODO: add timeouts and error handling to this.
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	r.Must(err, "Could not create cloud storage client")
	bucketHandle := client.Bucket(bucket)
	return &uploader{
		context:    ctx,
		namer:      namer,
		client:     client,
		bucket:     bucketHandle,
		bucketName: bucket,
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
