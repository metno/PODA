package kdvh

import "testing"

func TestFlagsAreValid(t *testing.T) {
	type testCase struct {
		input    Obs
		expected bool
	}

	cases := []testCase{
		{Obs{Flags: "12309"}, true},
		{Obs{Flags: "984.3"}, false},
		{Obs{Flags: ".1111"}, false},
		{Obs{Flags: "1234."}, false},
		{Obs{Flags: "12.2.4"}, false},
		{Obs{Flags: "12.343"}, false},
		{Obs{Flags: ""}, false},
		{Obs{Flags: "asdas"}, false},
		{Obs{Flags: "12a3a"}, false},
		{Obs{Flags: "1sdfl"}, false},
	}

	for _, c := range cases {
		t.Log("Testing flag:", c.input.Flags)

		if result := c.input.flagsAreValid(); result != c.expected {
			t.Errorf("Got %v, wanted %v", result, c.expected)
		}
	}
}
