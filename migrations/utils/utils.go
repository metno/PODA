package utils

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"slices"

	"github.com/schollz/progressbar/v3"
)

func NewBar(size int, description string) *progressbar.ProgressBar {
	return progressbar.NewOptions(size,
		progressbar.OptionOnCompletion(func() { fmt.Println() }),
		progressbar.OptionSetDescription(description),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(false),
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
	if slice == nil {
		return reference
	}

	if formatMsg == "" {
		formatMsg = "User input '%s' not present in reference, skipping"
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

func SetLogFile(tableName, procedure string) {
	filename := fmt.Sprintf("%s_%s_log.txt", tableName, procedure)
	fh, err := os.Create(filename)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create log '%s': %s", filename, err))
		return
	}
	log.SetOutput(fh)
}
