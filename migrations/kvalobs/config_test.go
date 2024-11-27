package kvalobs

import (
	"testing"

	"migrate/kvalobs/db"
)

func TestShouldProcessLabel(t *testing.T) {
	type TestCase[T string] struct {
		tag      string
		label    db.Label[T]
		config   db.BaseConfig[T]
		expected bool
	}

	cases := []TestCase[string]{
		{
			tag:      "empty config",
			label:    db.Label[string]{StationID: 18700},
			config:   db.BaseConfig[string]{},
			expected: true,
		},
		{
			tag:      "station specified",
			label:    db.Label[string]{StationID: 18700},
			config:   db.BaseConfig[string]{Stations: []int32{18700}},
			expected: true,
		},
		{
			tag:      "station not in label",
			label:    db.Label[string]{StationID: 18700},
			config:   db.BaseConfig[string]{Stations: []int32{20000}},
			expected: false,
		},
		{
			tag:      "label without level",
			label:    db.Label[string]{},
			config:   db.BaseConfig[string]{Levels: []int32{2}},
			expected: false,
		},
		{
			tag: "valid level",
			label: func() db.Label[string] {
				var level int32 = 2
				return db.Label[string]{Level: &level}
			}(),
			config:   db.BaseConfig[string]{Levels: []int32{2}},
			expected: true,
		},
	}

	for _, c := range cases {
		res := c.config.ShouldProcessLabel(&c.label)
		if res != c.expected {
			t.Log(c.tag)
			t.Fail()
		}
	}
}
