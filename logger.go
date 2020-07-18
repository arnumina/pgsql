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

	"github.com/arnumina/logger"
	"github.com/jackc/pgx/v4"
)

type (
	pgxLogger struct {
		*logger.Logger
	}
)

func (pl *pgxLogger) Log(_ context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	kv := []interface{}{}

	for key, value := range data {
		kv = append(kv, key)
		kv = append(kv, value)
	}

	switch level {
	case pgx.LogLevelTrace:
		pl.Trace(msg, kv...)
	case pgx.LogLevelDebug:
		pl.Debug(msg, kv...)
	case pgx.LogLevelInfo:
		pl.Info(msg, kv...)
	case pgx.LogLevelWarn:
		pl.Warning(msg, kv...)
	case pgx.LogLevelError:
		pl.Error(msg, kv...)
	}
}

/*
######################################################################################################## @(°_°)@ #######
*/
