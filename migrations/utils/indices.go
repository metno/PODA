package utils

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func DropIndices(pool *pgxpool.Pool) {
	slog.Info("Dropping table indices...")

	file, err := os.ReadFile("../db/drop_indices.sql")
	if err != nil {
		panic(err.Error())
	}

	_, err = pool.Exec(context.Background(), string(file))
	if err != nil {
		panic(err.Error())
	}
}

func CreateIndices(pool *pgxpool.Pool) {
	slog.Info("Recreating table indices...")

	files := []string{"../db/public.sql", "../db/flags.sql"}
	for _, filename := range files {
		file, err := os.ReadFile(filename)
		if err != nil {
			panic(err.Error())
		}

		_, err = pool.Exec(context.Background(), string(file))
		if err != nil {
			panic(err.Error())
		}
	}
}
