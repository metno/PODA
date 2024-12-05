package db

import (
	"testing"
)

func TestFlagsAreValid(t *testing.T) {
	type testCase struct {
		input    KdvhObs
		expected bool
	}

	cases := []testCase{
		{KdvhObs{Flags: "12309"}, true},
		{KdvhObs{Flags: "984.3"}, false},
		{KdvhObs{Flags: ".1111"}, false},
		{KdvhObs{Flags: "1234."}, false},
		{KdvhObs{Flags: "12.2.4"}, false},
		{KdvhObs{Flags: "12.343"}, false},
		{KdvhObs{Flags: ""}, false},
		{KdvhObs{Flags: "asdas"}, false},
		{KdvhObs{Flags: "12a3a"}, false},
		{KdvhObs{Flags: "1sdfl"}, false},
	}

	for _, c := range cases {
		t.Log("Testing flag:", c.input.Flags)

		if result := flagsAreValid(&c.input); result != c.expected {
			t.Errorf("Got %v, wanted %v", result, c.expected)
		}
	}
}
