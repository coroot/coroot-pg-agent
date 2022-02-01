package main

import (
	"github.com/coroot/coroot-pg-agent/collector"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/alecthomas/kingpin.v2"
	"net/http"
	_ "net/http/pprof"
)

var version = "unknown"

func main() {
	dsn := kingpin.Arg("dsn", `Data source name (env: DSN) - "postgresql://<user>:<password>@<host>:5432/postgres?connect_timeout=1&statement_timeout=30000".`).Envar("DSN").Required().String()
	listen := kingpin.Flag("listen", `Listen address (env: LISTEN) - "<ip>:<port>" or ":<port>".`).Envar("LISTEN").Default("0.0.0.0:80").String()
	kingpin.HelpFlag.Short('h').Hidden()
	kingpin.Version(version)
	kingpin.Parse()

	log := logger.NewKlog("")

	c, err := collector.New(*dsn, log)
	if err != nil {
		log.Error(err)
		return
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(info("pg_agent_info", version))
	reg.MustRegister(c)

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	log.Info("listening on:", *listen)
	log.Error(http.ListenAndServe(*listen, nil))
}

func info(name, version string) prometheus.Collector {
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        name,
		ConstLabels: prometheus.Labels{"version": version},
	})
	g.Set(1)
	return g
}
