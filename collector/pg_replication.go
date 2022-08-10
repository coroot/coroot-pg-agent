package collector

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/blang/semver"
	"net/url"
	"regexp"
	"strings"
)

var (
	valueRe    = `\s*=[\s']*([^\s']+)`
	hostRe     = regexp.MustCompile("host" + valueRe)
	hostaddrRe = regexp.MustCompile("hostaddr" + valueRe)
	portRe     = regexp.MustCompile("port" + valueRe)
)

func findValue(src string, re *regexp.Regexp) string {
	res := re.FindStringSubmatch(src)
	if len(res) < 2 {
		return ""
	}
	return res[1]
}

type replicationStatus struct {
	isInRecovery bool

	currentLsn sql.NullInt64
	receiveLsn sql.NullInt64
	replyLsn   sql.NullInt64

	isReplayPaused bool

	walReceiverStatus     int64
	primaryConnectionInfo string
}

func (rs *replicationStatus) primaryHostPort() (string, string, error) {
	ci := rs.primaryConnectionInfo
	if strings.HasPrefix(ci, "postgres://") || strings.HasPrefix(ci, "postgresql://") {
		u, err := url.Parse(ci)
		if err != nil {
			// don't log url.Parse errors since they might contain security sensitive data
			return "", "", fmt.Errorf("failed to parse primary_conninfo")
		}
		return u.Hostname(), u.Port(), nil
	}
	host := findValue(ci, hostRe)
	if host == "" {
		host = findValue(ci, hostaddrRe)
	}
	port := findValue(ci, portRe)
	return host, port, nil
}

func (c *Collector) getReplicationStatus(version semver.Version) (*replicationStatus, error) {
	var isInRecovery sql.NullBool
	if err := c.db.QueryRow(`SELECT pg_is_in_recovery()`).Scan(&isInRecovery); err != nil {
		return nil, err
	}

	if !isInRecovery.Valid {
		return nil, fmt.Errorf("pg_is_in_recovery() returned null")
	}

	var fCurrentLsn, fReceiveLsn, fReplyLsn, fIsReplayPaused string
	switch {
	// the `pg_stat_wal_receiver` view has been introduced in 9.6
	case semver.MustParseRange(">=9.6.0 <10.0.0")(version):
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

	rs := &replicationStatus{isInRecovery: isInRecovery.Bool}
	if rs.isInRecovery {
		if err := c.db.QueryRow(fmt.Sprintf(
			`SELECT %s()-'0/0', %s()-'0/0', %s()`, fReceiveLsn, fReplyLsn, fIsReplayPaused)).Scan(
			&rs.receiveLsn, &rs.replyLsn, &rs.isReplayPaused); err != nil {
			return nil, err
		}
		if err := c.db.QueryRow(`SELECT count(1) FROM pg_stat_wal_receiver`).Scan(&rs.walReceiverStatus); err != nil {
			return nil, err
		}
		if err := c.db.QueryRow(`SELECT setting FROM pg_settings WHERE name='primary_conninfo'`).Scan(&rs.primaryConnectionInfo); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		}
	} else {
		if err := c.db.QueryRow(fmt.Sprintf(`SELECT %s()-'0/0'`, fCurrentLsn)).Scan(&rs.currentLsn); err != nil {
			return nil, err
		}
	}
	return rs, nil
}
