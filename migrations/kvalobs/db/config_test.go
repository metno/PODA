package db

import (
	"testing"
)

func TestShouldProcessLabel(t *testing.T) {
	type TestCase struct {
		tag      string
		label    Label
		config   BaseConfig
		expected bool
	}

	cases := []TestCase{
		{
			tag:      "empty config",
			label:    Label{ParamID: 212},
			config:   BaseConfig{},
			expected: true,
		},
		{
			tag:      "label paramid in config paramids",
			label:    Label{ParamID: 212},
			config:   BaseConfig{ParamIds: []int32{212}},
			expected: true,
		},
		{
			tag:      "label paramid NOT in config paramids",
			label:    Label{ParamID: 212},
			config:   BaseConfig{ParamIds: []int32{300}},
			expected: false,
		},
		{
			tag:      "label level NOT in config level",
			label:    Label{},
			config:   BaseConfig{Levels: []int32{2}},
			expected: false,
		},
		{
			tag: "label level in config levels",
			label: func() Label {
				var level int32 = 2
				return Label{Level: &level}
			}(),
			config:   BaseConfig{Levels: []int32{2}},
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
