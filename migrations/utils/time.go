package utils

import (
	"fmt"
	"time"
)

type Timestamp struct {
	t time.Time
}

func (ts *Timestamp) UnmarshalText(b []byte) error {
	t, err := time.Parse(time.DateOnly, string(b))
	if err != nil {
		return fmt.Errorf("Only the date-only format (\"YYYY-MM-DD\") is allowed. Got %s", b)
	}
	ts.t = t
	return nil
}

func (ts *Timestamp) Format(layout string) string {
	return ts.t.Format(layout)
}

func (ts *Timestamp) Inner() *time.Time {
	if ts == nil {
		return nil
	}

	return &ts.t
}

type TimeSpan struct {
	From *time.Time
	To   *time.Time
}
