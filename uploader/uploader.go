package uploader

import (
	"bytes"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
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

// New creates and returns a new object that implements Uploader.
func New(project string, bucket string, namer namer.Namer) (*uploader, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	bucketHandle := client.Bucket(bucket)
	return &uploader{
		context:    ctx,
		namer:      namer,
		bucket:     bucketHandle,
		bucketName: bucket,
	}, nil
}

// Upload the provided buffer to GCS.
func (u *uploader) Upload(tarBuffer *bytes.Buffer) error {
	name := u.namer.ObjectName(time.Now().UTC())
	object := u.bucket.Object(name)
	writer := object.NewWriter(u.context)
	_, err := tarBuffer.WriteTo(writer)
	if err != nil {
		return fmt.Errorf("Could not write to gs://%s%s (%v)", u.bucketName, name, err)
	}
	return writer.Close()
}
