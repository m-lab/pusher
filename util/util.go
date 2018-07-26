// Package util provides free functions and type definitions that are
// generically useful in Go.  Arguably everything in here represents a gap of
// some sort in Go's standard library.
package util

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

// ArgsFromEnv will expand command-line argument parsing to include setting the
// values of flags from their corresponding environment variables. The
// environment variable for an argument is the upper-case version of the
// command-line flag.
func ArgsFromEnv(flagSet *flag.FlagSet) error {
	// Allow environment variables to be used for unspecified commandline flags.
	// Track what flags were explicitly set so that we won't override those flags.
	specifiedFlags := make(map[string]struct{})
	flagSet.Visit(func(f *flag.Flag) { specifiedFlags[f.Name] = struct{}{} })

	// All flags that were not explicitly set but do have a corresponding evironment variable should be set to that env value.
	// Visit every flag and don't override explicitly set commandline args.
	var err error
	flagSet.VisitAll(func(f *flag.Flag) {
		envVarName := strings.ToUpper(f.Name)
		if val, ok := os.LookupEnv(envVarName); ok {
			if _, specified := specifiedFlags[f.Name]; specified {
				log.Printf("WARNING: Not overriding flag -%s=%q with evironment variable %s=%q\n", f.Name, f.Value, envVarName, val)
			} else {
				if setErr := f.Value.Set(val); setErr != nil {
					err = fmt.Errorf("Could not set argument %s to the value of environment variable %s=%q (err: %s)", f.Name, envVarName, val, setErr)
				}
			}
		}
		log.Printf("Argument %s=%v\n", f.Name, f.Value)
	})
	return err
}

// Must will log.Fatal if passed a non-nil error. The fatal message is
// specified as the prefix argument. If any further args are passed, then the
// prefix will be treated as a format string.
//
// The main purpose of this function is to turn the common pattern of:
//    err := Func()
//    if err != nil {
//        log.Fatalf("Helpful message (error: %v)", err)
//    }
// into a simplified pattern of:
//    Must(Func(), "Helpful message")
func Must(err error, prefix string, args ...interface{}) {
	if err != nil {
		suffix := fmt.Sprintf("(error: %v)", err)
		if len(args) != 0 {
			prefix = fmt.Sprintf(prefix, args...)
		}
		log.Fatal(prefix, suffix)
	}
}
