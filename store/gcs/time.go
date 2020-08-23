package gcs

import (
	"fmt"
	"math/big"
	"time"
)

var nanosPerSecond, maxTimeNanos *big.Int

func timeToNanos(t time.Time) *big.Int {
	n := big.NewInt(t.Unix())
	n.Mul(n, nanosPerSecond)
	return n.Add(n, big.NewInt(int64(t.Nanosecond())))
}

func nanosToTime(n *big.Int) time.Time {
	var secs, nanos big.Int
	secs.DivMod(n, nanosPerSecond, &nanos)
	return time.Unix(secs.Int64(), nanos.Int64())
}

func timeToInvNanos(t time.Time) *big.Int {
	n := timeToNanos(t)
	return n.Sub(maxTimeNanos, n)
}

func invNanosToTime(n *big.Int) time.Time {
	var inv big.Int
	inv.Sub(maxTimeNanos, n)
	return nanosToTime(&inv)
}

func nanosToStr(n *big.Int) string {
	return fmt.Sprintf("%030s", n)
}

func strToNanos(s string) *big.Int {
	var n big.Int
	n.SetString(s, 10)
	return &n
}

func init() {
	// This is from https://stackoverflow.com/a/32620397
	maxTime := time.Unix(1<<63-1-int64((1969*365+1969/4-1969/100+1969/400)*24*60*60), 999999999)

	nanosPerSecond = big.NewInt(int64(time.Second))
	maxTimeNanos = timeToNanos(maxTime) // Must call after nanosPerSecond is initialized
}
