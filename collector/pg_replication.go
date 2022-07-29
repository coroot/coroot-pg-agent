package collector

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/blang/semver"
)

type replicationStatus struct {
	CurrentLsn     sql.NullInt64
	ReceiveLsn     sql.NullInt64
	ReplyLsn       sql.NullInt64
	IsReplayPaused sql.NullBool

	PrimaryConnectionInfo   sql.NullString
	PrimaryConnectionStatus sql.NullInt64
}

func (c *Collector) getReplicationStatus(version semver.Version) (*replicationStatus, error) {
	var isReplica sql.NullBool
	if err := c.db.QueryRow(`SELECT pg_is_in_recovery()`).Scan(&isReplica); err != nil {
		return nil, err
	}

	if !isReplica.Valid {
		return nil, fmt.Errorf("pg_is_in_recovery() returned null")
	}

	var fCurrentLsn, fReceiveLsn, fReplyLsn, fIsReplayPaused string
	switch {
	case semver.MustParseRange(">=9.4.0 <10.0.0")(version):
		fCurrentLsn = "pg_current_xlog_location"
		fReceiveLsn = "pg_last_xlog_receive_location"
		fReplyLsn = "pg_last_xlog_replay_location"
		fIsReplayPaused = "pg_is_xlog_replay_paused"
	case semver.MustParseRange(">=10.0.0")(version):
		fCurrentLsn = "pg_current_wal_lsn"
		fReceiveLsn = "pg_last_wal_receive_lsn"
		fReplyLsn = "pg_last_wal_replay_lsn"
		fIsReplayPaused = "pg_is_wal_replay_paused"
	default:
		return nil, fmt.Errorf("postgres version %s is not supported", version)
	}

	var res replicationStatus
	if isReplica.Bool {
		if err := c.db.QueryRow(fmt.Sprintf(
			`SELECT %s()-'0/0', %s()-'0/0', %s()`, fReceiveLsn, fReplyLsn, fIsReplayPaused)).Scan(
			&res.ReceiveLsn, &res.ReplyLsn, &res.IsReplayPaused); err != nil {
			return nil, err
		}
		if err := c.db.QueryRow(`SELECT count(1) FROM pg_stat_wal_receiver`).Scan(&res.PrimaryConnectionStatus); err != nil {
			return nil, err
		}
		if err := c.db.QueryRow(`SELECT setting FROM pg_settings WHERE name='primary_conninfo'`).Scan(&res.PrimaryConnectionInfo); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		}
	} else {
		if err := c.db.QueryRow(fmt.Sprintf(`SELECT %s()-'0/0'`, fCurrentLsn)).Scan(&res.CurrentLsn); err != nil {
			return nil, err
		}
	}
	return &res, nil
}
