package gcs

import (
	"reflect"
	"testing"
)

func TestEachHexPrefix(t *testing.T) {
	want := []string{
		"e67b", "e67c", "e67d", "e67e", "e67f",
		"e68", "e69", "e6a", "e6b", "e6c", "e6d", "e6e", "e6f",
		"e7", "e8", "e9", "ea", "eb", "ec", "ed", "ee", "ef",
		"f",
	}
	var got []string
	err := eachHexPrefix("e67a", false, func(prefix string) error {
		got = append(got, prefix)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
