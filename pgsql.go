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
	"context"
	"time"

	"github.com/arnumina/logger"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type (
	// Client AFAIRE.
	Client struct {
		logger *pgxLogger
		ctx    context.Context
		pool   *pgxpool.Pool
	}
)

// NewClient AFAIRE.
func NewClient(logger *logger.Logger) *Client {
	return &Client{
		logger: &pgxLogger{
			Logger: logger,
		},
		ctx: context.Background(),
	}
}

// ContexWithTimeout AFAIRE.
func (c *Client) ContexWithTimeout(t time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.ctx, t)
}

// Connect AFAIRE.
func (c *Client) Connect(uri string) error {
	config, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return err
	}

	config.ConnConfig.LogLevel = pgx.LogLevelWarn
	config.ConnConfig.Logger = c.logger

	ctx, cancel := c.ContexWithTimeout(5 * time.Second)
	defer cancel()

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return err
	}

	c.pool = pool

	return nil
}

// Exec AFAIRE.
func (c *Client) Exec(sql string, args ...interface{}) error {
	_, err := c.pool.Exec(c.ctx, sql, args...)
	return err
}

// Query AFAIRE.
func (c *Client) Query(sql string, args ...interface{}) (pgx.Rows, error) {
	return c.pool.Query(c.ctx, sql, args...)
}

// QueryRow AFAIRE.
func (c *Client) QueryRow(sql string, args ...interface{}) pgx.Row {
	return c.pool.QueryRow(c.ctx, sql, args...)
}

// Begin AFAIRE.
func (c *Client) Begin() (*Transaction, error) {
	tx, err := c.pool.Begin(c.ctx)
	if err != nil {
		return nil, err
	}

	t := &Transaction{
		client: c,
		tx:     tx,
	}

	return t, nil
}

// Close AFAIRE.
func (c *Client) Close() {
	if c.pool != nil {
		c.pool.Close() // BUG: doesn't give back the hand if the database is stopped and then restarted!
		c.pool = nil
	}
}

/*
######################################################################################################## @(°_°)@ #######
*/
