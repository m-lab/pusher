package bytecount

import (
	"flag"
	"testing"
)

func TestByteParsing(t *testing.T) {
	// Check successes
	type Pair struct {
		in  string
		out ByteCount
	}
	for _, pair := range []Pair{Pair{in: "1KB", out: ByteCount(1000)},
		Pair{in: "1B", out: ByteCount(1)},
		Pair{in: "2KB", out: ByteCount(2000)},
		Pair{in: "3MB", out: ByteCount(3000000)},
		Pair{in: "4GB", out: ByteCount(4000000000)},
		Pair{in: "5K", out: ByteCount(5000)},
		Pair{in: "6M", out: ByteCount(6000000)},
		Pair{in: "7G", out: ByteCount(7000000000)},
		Pair{in: "1000", out: ByteCount(1000)},
		Pair{in: "2", out: ByteCount(2)}} {
		b := ByteCount(0)
		if err := b.Set(pair.in); err != nil {
			t.Error(err)
		}
		if b.Get().(ByteCount) != pair.out {
			t.Errorf("Bad parse of %s (%d bytes != %d bytes)", pair.in, pair.out, b.Get().(ByteCount))
		}
	}
	// Check failures
	for _, input := range []string{"1 K", "1KB4BG", "K", "-3K", "abc12KB", "12KBabc"} {
		b := ByteCount(0)
		if err := b.Set(input); err == nil {
			t.Errorf("Failed to have an error on %q", input)
		}
	}
}

// Successful compilation of this function means that ByteCount implements the
// flag.Getter interface.
func assertFlagGetter(in flag.Getter) {
	var b ByteCount
	func(in flag.Getter) {}(&b)
}
