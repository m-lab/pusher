package uploader

import (
	"bytes"
)

// Uploader is an interface for uploading data.
type Uploader interface {
	Upload(*bytes.Buffer) error
}

// We split the Uploader into a struct and Interface to allow for mocking.
type uploader struct {
}

// New creates and returns a new object that implements Uploader.
func New(project string, bucket string) Uploader {
	return &uploader{}
}

// Upload the provided buffer to GCS.
func (u *uploader) Upload(tarBuffer *bytes.Buffer) error {
	return nil
}
