package outboxrepo

import (
	"database/sql"
)

func CreateOutbox(db *sql.DB) error {
	if _, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS foundation_outbox_events (
		id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
		topic TEXT NOT NULL,
		key TEXT NOT NULL,
		payload BYTEA NOT NULL,
		headers JSONB NOT NULL,
		created_at TIMESTAMPTZ NOT NULL
	);
`); err != nil {
		return err
	}

	return nil
}
