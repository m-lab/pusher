package fileinfo

import (
	"os"

	"github.com/m-lab/pusher/bytecount"
)

// A LocalDataFile holds all the information we require about a file.
type LocalDataFile struct {
	AbsoluteFileName string
	Info             os.FileInfo
	// Cache the size so sums of filesize are always using the same
	// integer value, in case the file grows in size between receipt and
	// upload.
	CachedSize bytecount.ByteCount
}
