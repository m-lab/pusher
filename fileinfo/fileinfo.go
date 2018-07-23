// Package fileinfo exists to provide a struct that is used in many parts of pusher.
package fileinfo

import (
	"os"
)

// A LocalDataFile holds all the information we require about a file.
type LocalDataFile struct {
	AbsoluteFileName string
	Info             os.FileInfo
}
