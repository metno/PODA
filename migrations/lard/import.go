package lard

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func InsertData(data DataInserter, pool *pgxpool.Pool, logStr string) (int64, error) {
	size := data.Len()
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromSlice(size, func(i int) ([]any, error) {
			return []any{
				data.ID(),
				data.Obstime(i),
				data.Data(i),
			}, nil
		}),
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

func InsertNonscalarData(data TextInserter, pool *pgxpool.Pool, logStr string) (int64, error) {
	size := data.Len()
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"public", "nonscalar_data"},
		[]string{"timeseries", "obstime", "obsvalue"},
		pgx.CopyFromSlice(size, func(i int) ([]any, error) {
			return []any{
				data.ID(),
				data.Obstime(i),
				data.Text(i),
			}, nil
		}),
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

func InsertFlags(data FlagInserter, pool *pgxpool.Pool, logStr string) error {
	size := data.Len()
	count, err := pool.CopyFrom(
		context.TODO(),
		pgx.Identifier{"flags", "kdvh"},
		[]string{"timeseries", "obstime", "controlinfo", "useinfo"},
		pgx.CopyFromSlice(size, func(i int) ([]any, error) {
			return []any{
				data.ID(),
				data.Obstime(i),
				data.Controlinfo(i),
				data.Useinfo(i),
			}, nil
		}),
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
