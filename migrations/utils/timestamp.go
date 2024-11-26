package utils

import "time"

type Timestamp struct {
	t time.Time
}

func (ts *Timestamp) UnmarshalText(b []byte) error {
	t, err := time.Parse(time.DateOnly, string(b))
	if err != nil {
		return err
	}
	ts.t = t
	return nil
}

func (ts *Timestamp) Format(layout string) string {
	return ts.t.Format(layout)
}
