package stinfosys

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

func getNonScalars(conn *pgx.Conn) []int32 {
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

// Tells if a paramid is scalar or not
type ScalarMap = map[int32]bool

func GetParamScalarMap(conn *pgx.Conn) ScalarMap {
	cache := make(ScalarMap)

	rows, err := conn.Query(context.TODO(), "SELECT paramid, scalar FROM param")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var paramid int32
		var isScalar bool
		if err := rows.Scan(&paramid, &isScalar); err != nil {
			log.Fatal(err)
		}
		cache[paramid] = isScalar
	}

	return cache

}
