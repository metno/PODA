package kvalobs

import (
	"migrate/kvalobs/db"
	"migrate/lard"
	"testing"
)

func TestShouldProcessLabel(t *testing.T) {
	type TestCase struct {
		tag      string
		label    lard.Label
		config   db.BaseConfig
		expected bool
	}

	cases := []TestCase{
		{
			tag:      "empty config",
			label:    lard.Label{StationID: 18700},
			config:   db.BaseConfig{},
			expected: true,
		},
		{
			tag:      "station specified",
			label:    lard.Label{StationID: 18700},
			config:   db.BaseConfig{Stations: []int32{18700}},
			expected: true,
		},
		{
			tag:      "station not in label",
			label:    lard.Label{StationID: 18700},
			config:   db.BaseConfig{Stations: []int32{20000}},
			expected: false,
		},
		{
			tag:      "label without level",
			label:    lard.Label{},
			config:   db.BaseConfig{Levels: []int32{2}},
			expected: false,
		},
		{
			tag: "valid level",
			label: func() lard.Label {
				var level int32 = 2
				return lard.Label{Level: &level}
			}(),
			config:   db.BaseConfig{Levels: []int32{2}},
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
