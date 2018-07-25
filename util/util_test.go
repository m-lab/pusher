package util

import (
	"flag"
	"os"
	"testing"
)

func TestArgsFromEnvDefaults(t *testing.T) {
	flagSet := flag.NewFlagSet("test_flags", flag.ContinueOnError)
	flagVal := flagSet.String("pusher_util_test_var", "default", "")
	flagSet.Parse([]string{})
	if err := ArgsFromEnv(flagSet); err != nil {
		t.Error(err)
	}
	if *flagVal != "default" {
		t.Error("Bad flag value", *flagVal)
	}
}

func TestArgsFromEnvSpecifiedNoEnv(t *testing.T) {
	flagSet := flag.NewFlagSet("test_flags", flag.ContinueOnError)
	flagVal := flagSet.String("pusher_util_test_var", "default", "")
	flagSet.Parse([]string{"-pusher_util_test_var=value_from_cmdline"})
	if err := ArgsFromEnv(flagSet); err != nil {
		t.Error(err)
	}
	if *flagVal != "value_from_cmdline" {
		t.Error("Bad flag value", *flagVal)
	}
}

func TestArgsFromEnvNotSpecifiedYesEnv(t *testing.T) {
	flagSet := flag.NewFlagSet("test_flags", flag.ContinueOnError)
	flagVal := flagSet.String("pusher_util_test_var", "default", "")
	oldVal, ok := os.LookupEnv("PUSHER_UTIL_TEST_VAR")
	os.Setenv("PUSHER_UTIL_TEST_VAR", "value_from_env")
	defer func() {
		if ok {
			os.Setenv("PUSHER_UTIL_TEST_VAR", oldVal)
		} else {
			os.Unsetenv("PUSHER_UTIL_TEST_VAR")
		}
	}()
	flagSet.Parse([]string{})
	if err := ArgsFromEnv(flagSet); err != nil {
		t.Error(err)
	}
	if *flagVal != "value_from_env" {
		t.Error("Bad flag value", *flagVal)
	}
}

func TestArgsFromEnvWontOverride(t *testing.T) {
	flagSet := flag.NewFlagSet("test_flags", flag.ContinueOnError)
	flagVal := flagSet.String("pusher_util_test_var", "default", "")
	oldVal, ok := os.LookupEnv("PUSHER_UTIL_TEST_VAR")
	os.Setenv("PUSHER_UTIL_TEST_VAR", "value_from_env")
	defer func() {
		if ok {
			os.Setenv("PUSHER_UTIL_TEST_VAR", oldVal)
		} else {
			os.Unsetenv("PUSHER_UTIL_TEST_VAR")
		}
	}()
	flagSet.Parse([]string{"-pusher_util_test_var=value_from_cmdline"})
	if err := ArgsFromEnv(flagSet); err != nil {
		t.Error(err)
	}
	if *flagVal != "value_from_cmdline" {
		t.Error("Bad flag value", *flagVal)
	}
}
