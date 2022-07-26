package main

import (
	"github.com/coroot/coroot-pg-agent/collector"
	"github.com/coroot/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/alecthomas/kingpin.v2"
	"k8s.io/klog/v2"
	"net/http"
	_ "net/http/pprof"
)

var version = "unknown"

func main() {
	dsn := kingpin.Arg("dsn", `Data source name (env: DSN) - "postgresql://<user>:<password>@<host>:5432/postgres?connect_timeout=1&statement_timeout=30000".`).Envar("DSN").Required().String()
	listen := kingpin.Flag("listen", `Listen address (env: LISTEN) - "<ip>:<port>" or ":<port>".`).Envar("LISTEN").Default("0.0.0.0:80").String()
	scrapeInterval := kingpin.Flag("scrape-interval", `How often to snapshot system views (env: PG_SCRAPE_INTERVAL)`).Envar("PG_SCRAPE_INTERVAL").Default("15s").Duration()
	staticLabels := kingpin.Flag("label", `A static label:value pair to be added to all metrics (env: STATIC_LABELS)`).Envar("STATIC_LABELS").StringMap()

	kingpin.HelpFlag.Short('h').Hidden()
	kingpin.Version(version)
	kingpin.Parse()

	log := logger.NewKlog("")

	c, err := collector.New(*dsn, *scrapeInterval, log)
	if err != nil {
		log.Error(err)
		return
	}

	registry := prometheus.NewRegistry()

	klog.Infoln("static labels:", *staticLabels)
	registerer := prometheus.WrapRegistererWith(*staticLabels, registry)
	registerer.MustRegister(info("pg_agent_info", version))
	registerer.MustRegister(c)

	http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
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
