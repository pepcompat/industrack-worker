package mqtt

import (
	"strconv"
	"time"
)

// parseTime supports RFC3339 string or Unix timestamp (seconds).
func parseTime(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err == nil {
		return &t, nil
	}
	sec, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		t := time.Unix(sec, 0)
		return &t, nil
	}
	return nil, err
}
