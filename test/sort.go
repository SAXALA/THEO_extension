// sort.go
// Generate N random strings (length 33), store in []string, sort and measure time.
// Usage: go run sort.go N [runs] [seed]

package main

import (
	crand "crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"
)

func genRandomString(lenBytes int, r *rand.Rand) string {
	const charset = "0123456789abcdef"
	b := make([]byte, lenBytes)
	for i := 0; i < lenBytes; i++ {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s N [runs] [seed]\n", os.Args[0])
		os.Exit(1)
	}

	var N int
	_, err := fmt.Sscan(args[0], &N)
	if err != nil || N < 0 {
		fmt.Fprintf(os.Stderr, "Invalid N: %s\n", args[0])
		os.Exit(1)
	}

	runs := 1
	if len(args) >= 2 {
		_, err := fmt.Sscan(args[1], &runs)
		if err != nil || runs < 1 {
			runs = 1
		}
	}

	var seed int64
	if len(args) >= 3 {
		_, err := fmt.Sscan(args[2], &seed)
		if err != nil {
			seed = time.Now().UnixNano()
		}
	} else {
		// generate a random seed from crypto/rand for better variance
		var b [8]byte
		if _, err := crand.Read(b[:]); err == nil {
			seed = int64(binary.LittleEndian.Uint64(b[:]))
		} else {
			seed = time.Now().UnixNano()
		}
	}

	fmt.Printf("N=%d runs=%d seed=%d\n", N, runs, seed)

	const KEY_LEN = 33
	const VAL_LEN = 600

	type Record struct {
		Key   string
		Value string
	}

	for runc := 0; runc < runs; runc++ {
		rng := rand.New(rand.NewSource(seed + int64(runc)))
		arr := make([]Record, 0, N)
		for i := 0; i < N; i++ {
			rec := Record{
				Key:   genRandomString(KEY_LEN, rng),
				Value: genRandomString(VAL_LEN, rng),
			}
			arr = append(arr, rec)
		}

		t0 := time.Now()
		sort.Slice(arr, func(i, j int) bool { return arr[i].Key < arr[j].Key })
		t1 := time.Now()
		dur := t1.Sub(t0).Seconds() * 1000.0
		fmt.Printf("run %d: sort time = %.6f ms\n", runc+1, dur)
		if len(arr) > 0 {
			fmt.Printf("first_key=%s last_key=%s\n", arr[0].Key[:6], arr[len(arr)-1].Key[:6])
		}
	}
}
