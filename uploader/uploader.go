package uploader

import (
	"bytes"
)

// TODO: implement these functions.

// Uploader is an interface for uploading data.
type Uploader interface {
	Upload(*bytes.Buffer) error
}

// We split the Uploader into a struct and Interface to allow for mocking.
type uploader struct {
	template string
}

// New creates and returns a new object that implements Uploader.
func New(template string) Uploader {
	return &uploader{}
}

// Upload the provided buffer to GCS.
func (u *uploader) Upload(tarBuffer *bytes.Buffer) error {
	return nil
}
