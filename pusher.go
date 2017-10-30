package pusher

import (
	"os"
)

type LocalDataFile struct {
	fullRelativeName string
	info os.FileInfo
	cachedSize int64
}
