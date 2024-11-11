package lard

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TODO: I'm not sure I like the interface solution
type DataInserter interface {
	InsertData(i int) ([]any, error)
	Len() int
}

type TextInserter interface {
	InsertText(i int) ([]any, error)
	Len() int
}

type FlagInserter interface {
	InsertFlags(i int) ([]any, error)
	Len() int
}

func InsertData(ts DataInserter, pool *pgxpool.Pool, logStr string) (int64, error) {
	size := ts.Len()
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromSlice(size, ts.InsertData),
	)
	if err != nil {
		return count, err
	}

	logStr += fmt.Sprintf("%v/%v data rows inserted", count, size)
	if int(count) != size {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return count, nil
}

func InsertTextData(ts TextInserter, pool *pgxpool.Pool, logStr string) (int64, error) {
	size := ts.Len()
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "nonscalar_data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromSlice(size, ts.InsertText),
	)
	if err != nil {
		return count, err
	}

	logStr += fmt.Sprintf("%v/%v non-scalar data rows inserted", count, size)
	if int(count) != size {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return count, nil
}

func InsertFlags(ts FlagInserter, table pgx.Identifier, columns []string, pool *pgxpool.Pool, logStr string) error {
	size := ts.Len()
	count, err := pool.CopyFrom(
		context.TODO(),
		table,
		columns,
		pgx.CopyFromSlice(size, ts.InsertFlags),
	)
	if err != nil {
		return err
	}

	logStr += fmt.Sprintf("%v/%v flag rows inserted", count, size)
	if int(count) != size {
		slog.Warn(logStr)
	} else {
		slog.Info(logStr)
	}
	return nil
}
