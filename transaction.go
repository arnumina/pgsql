/*
#######
##                             __
##         ___  ___ ____ ___ _/ /
##        / _ \/ _ `(_-</ _ `/ /
##       / .__/\_, /___/\_, /_/
##      /_/   /___/      /_/
##
####### (c) 2020 Institut National de l'Audiovisuel ######################################## Archivage Numérique #######
*/

package pgsql

import (
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type (
	// Transaction AFAIRE.
	Transaction struct {
		client *Client
		tx     pgx.Tx
	}
)

// Rollback AFAIRE.
func (t *Transaction) Rollback() {
	_ = t.tx.Rollback(t.client.ctx)
}

// Commit AFAIRE.
func (t *Transaction) Commit() error {
	return t.tx.Commit(t.client.ctx)
}

// Exec AFAIRE.
func (t *Transaction) Exec(sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return t.tx.Exec(t.client.ctx, sql, args...)
}

// Query AFAIRE.
func (t *Transaction) Query(sql string, args ...interface{}) (pgx.Rows, error) {
	return t.tx.Query(t.client.ctx, sql, args...)
}

// QueryRow AFAIRE.
func (t *Transaction) QueryRow(sql string, args ...interface{}) pgx.Row {
	return t.tx.QueryRow(t.client.ctx, sql, args...)
}

// TryLock AFAIRE.
func (t *Transaction) TryLock(key int) (bool, error) {
	var locked bool

	if err := t.QueryRow("SELECT pg_try_advisory_xact_lock($1)", key).Scan(&locked); err != nil {
		return false, err
	}

	return locked, nil
}

/*
######################################################################################################## @(°_°)@ #######
*/
