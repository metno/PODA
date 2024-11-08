package kvalobs

import (
	"log/slog"
	"os"
	"slices"
	"strconv"

	"github.com/gocarina/gocsv"
)

// Loads a CSV file where records (lines) are described by type T
func readCSVfile[T any](filename string) ([]T, error) {
	file, err := os.Open(filename)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}
	defer file.Close()

	// TODO: maybe I should preallocate slice size if I can?
	// Does UnmarshalFile allocate?
	// labels := make([]T, 0, size)
	var labels []T
	err = gocsv.UnmarshalFile(file, &labels)
	if err != nil {
		slog.Error(err.Error())
		return nil, err
	}

	return labels, nil
}

func toInt32(s string) int32 {
	res, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		// Panic is fine here, because we use this function only at startup
		panic("Could not parse to int")
	}
	return int32(res)
}

func Map[T, V any](ts []T, fn func(T) V) []V {
	result := make([]V, len(ts))
	for i, t := range ts {
		result[i] = fn(t)
	}
	return result
}

// Similar to Map, but bails immediately if an error occurs
func TryMap[T, V any](ts []T, fn func(T) (V, error)) ([]V, error) {
	result := make([]V, len(ts))
	for i, t := range ts {
		temp, err := fn(t)
		if err != nil {
			return nil, err
		}
		result[i] = temp
	}
	return result, nil
}

func contains[T comparable](s []T, v T) bool {
	if s == nil {
		return true
	}
	return slices.Contains(s, v)
}

// Returns true if the slice is empty or the value is null
func nullableContains[T comparable](s []T, v *T) bool {
	if s == nil || v == nil {
		return true
	}
	return slices.Contains(s, *v)
}
