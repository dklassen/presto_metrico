package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ooyala/go-dogstatsd"
	"github.com/pborman/getopt"
)

var (
	usage = fmt.Sprintf(`
	Presto Metrico  - Collect and send Presto Metrics to datadog

	OPTIONS:
 -c
 The coordinator node we are going to pull metrics from
 -d
 The uri for the statsd client. Defaults to 127.0.0.1:8125
 -t
 The time in secs between sending metrics
`)

	commandOptions      = getopt.New()
	coordinatorOpts     = commandOptions.StringLong("coordinator", 'c', "", "Address of the Presto coordinator")
	dogstatsdServerOpts = commandOptions.StringLong("dogstatsd", 'd', "127.0.0.1:8125", "Address for the statsd server")
	metricsIntervalOpts = commandOptions.IntLong("timer", 't', 15, "Time in seconds to trigger timer to send metrics")
)

func printHelp() {
	log.Println(usage)
	os.Exit(0)
}

func main() {
	if len(os.Args) < 2 {
		printHelp()
	}

	commandOptions.Parse(os.Args)
	os.Setenv("PRESTO_COORDINATOR", *coordinatorOpts)
	log.Println("Starting Presto Metrico")

	client, err := dogstatsd.New(*dogstatsdServerOpts)

	if err != nil {
		log.Fatal(err)
	}

	seconds := time.Duration(*metricsIntervalOpts)

	t := time.NewTicker(seconds * time.Second)
	for now := range t.C {
		log.Println("Sending metrics: ", now)
		ProcessJMXMetrics(client)
	}
}
