// Package bytecount provides a single datatype ByteCount, designed to track
// counts of bytes, as well as some helper constants.  It also provides all the
// necessary functions to allow a ByteCount to be specified as a command-line
// argument, which should allow command-line arguments like
// `--cache-size=20MB`.
package bytecount

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
)

// ByteCount holds filesizes and the like.
type ByteCount int64

// Some constants to make working with ByteCounts easier.
const (
	Byte     ByteCount = 1
	Kilobyte           = 1000 * Byte
	Megabyte           = 1000 * Kilobyte
	Gigabyte           = 1000 * Megabyte
)

// Get is used by the Flag library to get the value out of the ByteCount.
func (b ByteCount) Get() interface{} {
	return b
}

// String is used by the Flag library to turn the value into a string.  For
// ease of implementation we avoid attempting to turn the string into a short
// KB/MB/GB form, instead electing to just keep everything expanded even if it
// could be expressed more succinctly.
func (b ByteCount) String() string {
	if b%Gigabyte == 0 {
		return fmt.Sprintf("%dGB", b/Gigabyte)
	} else if b%Megabyte == 0 {
		return fmt.Sprintf("%dMB", b/Megabyte)
	} else if b%Kilobyte == 0 {
		return fmt.Sprintf("%dKB", b/Kilobyte)
	}
	return fmt.Sprintf("%dB", b)
}

// Set is used by the Flag library to turn a string into a ByteCount.  This
// parses on the quick and dirty using regular expressions.
func (b *ByteCount) Set(s string) error {
	bytesRegexp := regexp.MustCompile(`^(?P<quantity>[0-9]+)(?P<units>[KMG]?B?)?$`)
	if !bytesRegexp.MatchString(s) {
		return fmt.Errorf("Invalid size format: %q", s)
	}
	for _, submatches := range bytesRegexp.FindAllStringSubmatchIndex(s, -1) {
		quantityBytes := bytesRegexp.ExpandString([]byte{}, "$quantity", s, submatches)
		quantityInt, err := strconv.ParseInt(string(quantityBytes), 10, 64)
		if err != nil {
			return err
		}
		quantity := ByteCount(quantityInt)
		unitsBytes := bytesRegexp.ExpandString([]byte{}, "$units", s, submatches)
		units := Byte
		switch string(unitsBytes) {
		case "B", "":
			units = Byte
		case "KB", "K":
			units = Kilobyte
		case "MB", "M":
			units = Megabyte
		case "GB", "G":
			units = Gigabyte
		default:
			log.Fatalf("The string %q passed the regexp %q but did not have units we recognize. There is an error in the regexp or the code.", s, bytesRegexp.String())
		}
		*b = quantity * units
	}
	return nil
}
