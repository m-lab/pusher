package pusher

import (
	"os"
)

type LocalDataFile struct {
	FullRelativeName string
	Info os.FileInfo
	// Cache the size so sums of filesize are always using the same
	// integer value, in case the file grows in size between walk and
	// upload.
	CachedSize int64
}
