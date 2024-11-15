package port

import "testing"

func TestFlagsAreValid(t *testing.T) {
	type testCase struct {
		input    KdvhObs
		expected bool
	}

	cases := []testCase{
		{KdvhObs{flags: "12309"}, true},
		{KdvhObs{flags: "984.3"}, false},
		{KdvhObs{flags: ".1111"}, false},
		{KdvhObs{flags: "1234."}, false},
		{KdvhObs{flags: "12.2.4"}, false},
		{KdvhObs{flags: "12.343"}, false},
		{KdvhObs{flags: ""}, false},
		{KdvhObs{flags: "asdas"}, false},
		{KdvhObs{flags: "12a3a"}, false},
		{KdvhObs{flags: "1sdfl"}, false},
	}

	for _, c := range cases {
		t.Log("Testing flag:", c.input.flags)

		if result := c.input.flagsAreValid(); result != c.expected {
			t.Errorf("Got %v, wanted %v", result, c.expected)
		}
	}
}
