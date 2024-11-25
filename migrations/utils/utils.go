package utils

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/schollz/progressbar/v3"
)

func NewBar(size int, description string) *progressbar.ProgressBar {
	return progressbar.NewOptions(size,
		progressbar.OptionOnCompletion(func() { fmt.Println() }),
		progressbar.OptionSetDescription(description),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(false),
		progressbar.OptionSetElapsedTime(true),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)
}

// Filters elements of a slice by comparing them to the elements of a reference slice.
// formatMsg is an optional format string with a single format argument that can be used
// to add context on why the element may be missing from the reference slice
func FilterSlice[T comparable](slice, reference []T, formatMsg string) []T {
	if len(slice) == 0 {
		return reference
	}

	if formatMsg == "" {
		formatMsg = "Value '%s' not present in reference slice, skipping"
	}

	// I hate this so much
	out := slice[:0]
	for _, s := range slice {
		if !slices.Contains(reference, s) {
			slog.Warn(fmt.Sprintf(formatMsg, s))
			continue
		}
		out = append(out, s)
	}
	return out
}

// Saves a slice to a file
func SaveToFile(values []string, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	file.WriteString(strings.Join(values, "\n"))
	return file.Close()
}

func SetLogFile(table, procedure string) {
	filename := fmt.Sprintf("%s_%s_log.txt", table, procedure)
	fh, err := os.Create(filename)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create log '%s': %s", filename, err))
		return
	}
	log.SetOutput(fh)
}

func ToInt32(s string) int32 {
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

func Contains[T comparable](s []T, v T) bool {
	if s == nil {
		return true
	}
	return slices.Contains(s, v)
}

// Returns true if the slice is empty or the value is null
func NullableContains[T comparable](s []T, v *T) bool {
	if s == nil || v == nil {
		return true
	}
	return slices.Contains(s, *v)
}
