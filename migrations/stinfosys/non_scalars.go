package stinfosys

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

func GetNonScalars(conn *pgx.Conn) []int32 {
	rows, err := conn.Query(context.TODO(), "SELECT paramid FROM param WHERE scalar = false ORDER BY paramid")
	if err != nil {
		log.Fatal(err)
	}
	nonscalars, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		log.Fatal(err)
	}
	return nonscalars
}
