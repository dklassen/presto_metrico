package main

import (
	"fmt"
	"github.com/dklassen/presto_metrico"
	"github.com/ooyala/go-dogstatsd"
	"github.com/pborman/getopt"
	"log"
	"os"
	"time"
)

var (
	usage = fmt.Sprintf(`NAME
	Presto Metrico  - Presto metrics and information thing
OPTIONS:
 -server
  The coordinator node we are going to pull metrics from
`, os.Args[0])

	commandOptions = getopt.New()
	servername     = commandOptions.StringLong("server", 's', "defval", "Address of the Presto coordinator")
)

func printHelp() {
	log.Println(os.Stderr, usage)
	os.Exit(0)
}

func main() {
	if len(os.Args) < 2 {
		printHelp()
	}

	commandOptions.Parse(os.Args)
	presto_metrico.Configure(*servername)

	log.Println("Starting Presto Metrico")
	client, err := dogstatsd.New("127.0.0.1:8125")

	if err != nil {
		log.Fatal(err)
	}

	t := time.NewTicker(10 * time.Second)
	for now := range t.C {
		log.Println("Sending metrics: ", now)
		presto_metrico.SendJMXMetrics(client)
	}
}
