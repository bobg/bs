package gcs

import (
	"math/big"
	"testing"
	"testing/quick"
	"time"
)

func TestTime(t *testing.T) {
	tests := []struct {
		name string
		fn   interface{}
	}{
		{
			name: "reversible 1",
			fn: func(nanos int64) bool {
				tm := nanosToTime(big.NewInt(nanos))
				got := timeToNanos(tm)
				return got.Int64() == nanos
			},
		},
		{
			name: "reversible 2",
			fn: func(secs, nsecs int64) bool {
				tm := randToTime(secs, nsecs)
				inv := timeToInvNanos(tm)
				invStr := nanosToStr(inv)
				inv2 := strToNanos(invStr)
				tm2 := invNanosToTime(inv2)
				return tm.Equal(tm2)
			},
		},
		{
			name: "ordering",
			fn: func(s1, n1, s2, n2 int64) bool {
				t1 := randToTime(s1, n1)
				t2 := randToTime(s2, n2)
				invStr1 := nanosToStr(timeToInvNanos(t1))
				invStr2 := nanosToStr(timeToInvNanos(t2))
				if t1.Before(t2) {
					return invStr1 > invStr2
				}
				if t1.After(t2) {
					return invStr1 < invStr2
				}
				return invStr1 == invStr2
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := quick.Check(tt.fn, nil)
			if err != nil {
				t.Error(err)
			}
		})
	}
}

func randToTime(secs, nsecs int64) time.Time {
	nsecs %= int64(time.Second)
	return time.Unix(secs, nsecs)
}
