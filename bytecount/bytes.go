package bytecount

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
)

// ByteCount holds filesizes and the like.
type ByteCount int64

// Some constants to make working with ByteCounts easier.
const (
	Byte	 ByteCount = 1
	Kilobyte           = 1000 * Byte
	Megabyte           = 1000 * Kilobyte
	Gigabyte           = 1000 * Megabyte
)

// Get is used by the Flag library to get the value out of the ByteCount.
func (b *ByteCount) Get() interface{} {
	return *b
}

// String is used by the Flag library to turn the value into a string.  For
// ease of implementation we avoid attempting to turn the string into a short
// KB/MB/GB form, instead electing to just keep everything expanded even if it
// could be expressed more succinctly.
func (b *ByteCount) String() string {
	return fmt.Sprintf("%d", *b)
}

// Set is used by the Flag library to turn a string into a ByteCount.  This
// parses on the quick and dirty using regular expressions.
func (b *ByteCount) Set(s string) error {
	bytesRegexp := regexp.MustCompile(`^(?P<quantity>[0-9]+)(?P<units>[KMG]?B?)?$`)
	if !bytesRegexp.MatchString(s) {
		return errors.New("No match for " + s)
	}
	for _, submatches := range bytesRegexp.FindAllStringSubmatchIndex(s, -1) {
		quantityBytes := []byte{}
		quantityBytes = bytesRegexp.ExpandString(quantityBytes, "$quantity", s, submatches)
		quantityInt, err := strconv.ParseInt(string(quantityBytes), 10, 64)
		if err != nil {
			return err
		}
		quantity := ByteCount(quantityInt)
		unitsBytes := []byte{}
		unitsBytes = bytesRegexp.ExpandString(unitsBytes, "$units", s, submatches)
		units := ByteCount(1)
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
