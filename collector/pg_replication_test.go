package collector

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_parsePrimaryConnectionInfo(t *testing.T) {
	check := func(src string, host, port string) {
		rs := replicationStatus{primaryConnectionInfo: src}
		h, p, err := rs.primaryHostPort()
		assert.NoError(t, err)
		assert.Equal(t, host, h)
		assert.Equal(t, port, p)
	}

	check("host=127.0.0.1 port=5432", "127.0.0.1", "5432")
	check("host=127.0.0.1", "127.0.0.1", "")

	check("host = 127.0.0.1 port = 5432", "127.0.0.1", "5432")
	check("host = '127.0.0.1' port = 5432", "127.0.0.1", "5432")
	check("host = ' 127.0.0.1 ' port = 5432", "127.0.0.1", "5432")

	check("hostaddr=127.0.0.1 port=5432", "127.0.0.1", "5432")

	check("postgresql://localhost:5433", "localhost", "5433")
	check("postgres://localhost:5433", "localhost", "5433")
	check("postgresql://user:secret@localhost", "localhost", "")
	check("postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp", "localhost", "")
	check("postgresql://[2001:db8::1234]/database", "2001:db8::1234", "")
}
