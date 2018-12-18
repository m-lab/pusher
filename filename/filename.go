package filename

import (
	"fmt"
	"log"
	"path"
	"regexp"
	"strings"
)

// System contains a filename suitable for passing directly to os.Remove.
type System string

// Internal removes the path information that is not relevant to the tarfile.
func (s System) Internal(rootDirectory System) Internal {
	return Internal(strings.TrimPrefix(string(s), string(rootDirectory)))
}

// Internal is the pathname of a data file inside of the tarfile.
type Internal string

// Subdir returns the subdirectory of the Internal filename, up to 3 levels
// deep. It is only guaranteed to work right on relative path names, suitable
// for inclusion in tarfiles.
func (l Internal) Subdir() string {
	dirs := strings.Split(string(l), "/")
	if len(dirs) <= 1 {
		log.Printf("File handed to the tarcache is not in a subdirectory: %v is not split by /", l)
		return ""
	}
	k := len(dirs) - 1
	if k > 3 {
		k = 3
	}
	return strings.Join(dirs[:k], "/")
}

// Lint returns nil if the file has a normal name, and an explanatory error
// about why the name is strange otherwise.
func (l Internal) Lint() error {
	name := string(l)
	cleaned := path.Clean(name)
	if cleaned != name {
		return fmt.Errorf("The cleaned up path %q did not match the name of the passed-in file %q", cleaned, name)
	}
	d, f := path.Split(name)
	if strings.HasPrefix(f, ".") {
		return fmt.Errorf("Hidden file detected: %q", name)
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("Too many dots in %v", name)
	}
	invalidChars := regexp.MustCompile(`[^a-zA-Z0-9/:._-]`)
	if invalidChars.MatchString(name) {
		return fmt.Errorf("Strange characters detected in the filename %q", name)
	}
	recommendedFormat := regexp.MustCompile(`^[a-zA-Z0-9_-]+/20[0-9][0-9]/[0-9]{2}/[0-9]{2}`)
	if !recommendedFormat.MatchString(d) {
		return fmt.Errorf("Directory structure does not mirror our best practices for file %v", name)
	}
	return nil
}
