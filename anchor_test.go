package bs

import (
	"fmt"
	"testing"
	"time"
)

func TestFindAnchor(t *testing.T) {
	t1, err := time.Parse(time.RFC3339, "1977-08-05T13:00:00-04:00")
	if err != nil {
		t.Fatal(err)
	}
	t2 := t1.Add(time.Hour)

	r1 := Ref{1}
	r2 := Ref{2}

	cases := []struct {
		pairs   []TimeRef
		at      time.Time
		want    Ref
		wantErr bool
	}{
		{
			at:      t1,
			wantErr: true,
		},
		{
			pairs: []TimeRef{{T: t1, R: r1}},
			at:    t1,
			want:  r1,
		},
		{
			pairs:   []TimeRef{{T: t1, R: r1}},
			at:      t1.Add(-time.Minute),
			wantErr: true,
		},
		{
			pairs: []TimeRef{{T: t1, R: r1}},
			at:    t1.Add(time.Minute),
			want:  r1,
		},
		{
			pairs: []TimeRef{{T: t1, R: r1}, {T: t2, R: r2}},
			at:    t1,
			want:  r1,
		},
		{
			pairs:   []TimeRef{{T: t1, R: r1}, {T: t2, R: r2}},
			at:      t1.Add(-time.Minute),
			wantErr: true,
		},
		{
			pairs: []TimeRef{{T: t1, R: r1}, {T: t2, R: r2}},
			at:    t1.Add(time.Minute),
			want:  r1,
		},
		{
			pairs: []TimeRef{{T: t1, R: r1}, {T: t2, R: r2}},
			at:    t2,
			want:  r2,
		},
		{
			pairs: []TimeRef{{T: t1, R: r1}, {T: t2, R: r2}},
			at:    t2.Add(time.Minute),
			want:  r2,
		},
	}

	for i, tc := range cases {
		t.Run(fmt.Sprintf("case_%02d", i+1), func(t *testing.T) {
			got, err := FindAnchor(tc.pairs, tc.at)
			switch {
			case tc.wantErr && err == nil:
				t.Error("got no error, want one")
			case !tc.wantErr && err != nil:
				t.Errorf("got error %v, want no error", err)
			case tc.wantErr:
				// ok - want error, got one
			case got != tc.want:
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}
