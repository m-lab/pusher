package uploader

import (
	"bytes"
)

type Uploader struct {
}

func New(project string, bucket string, creds string) *Uploader {
	return &Uploader{}
}

func (u *Uploader) Upload(tarBuffer *bytes.Buffer) error {
	return nil
}
