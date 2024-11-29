package db

import (
	"testing"
)

func TestShouldProcessLabel(t *testing.T) {
	type TestCase[T string] struct {
		tag      string
		label    Label[T]
		config   BaseConfig[T]
		expected bool
	}

	cases := []TestCase[string]{
		{
			tag:      "empty config",
			label:    Label[string]{StationID: 18700},
			config:   BaseConfig[string]{},
			expected: true,
		},
		{
			tag:      "station specified",
			label:    Label[string]{StationID: 18700},
			config:   BaseConfig[string]{Stations: []int32{18700}},
			expected: true,
		},
		{
			tag:      "station not in label",
			label:    Label[string]{StationID: 18700},
			config:   BaseConfig[string]{Stations: []int32{20000}},
			expected: false,
		},
		{
			tag:      "label without level",
			label:    Label[string]{},
			config:   BaseConfig[string]{Levels: []int32{2}},
			expected: false,
		},
		{
			tag: "valid level",
			label: func() Label[string] {
				var level int32 = 2
				return Label[string]{Level: &level}
			}(),
			config:   BaseConfig[string]{Levels: []int32{2}},
			expected: true,
		},
	}

	for _, c := range cases {
		t.Log(c.tag)
		res := c.config.ShouldProcessLabel(&c.label)
		if res != c.expected {
			t.Fail()
		}
	}
}
