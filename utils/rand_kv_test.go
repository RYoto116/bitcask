package utils

import "testing"

func TestGetTestKey(t *testing.T) {
	for i := range 5 {
		t.Log(string(GetTestKey(i)))
	}
}

func TestRandomValue(t *testing.T) {
	for i := range 5 {
		t.Log(string(RandomValue(i + 1)))
	}
}
