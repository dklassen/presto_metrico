package main

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	Setup()
	os.Exit(m.Run())
}

func Setup() {
	os.Setenv("PRESTO_COORDINATOR", "testomcpresto")
	jmxBeans = map[string]string{"woot_zone": "this_is_a_test"}
}

func TestBuildMetricUri(t *testing.T) {
	testString := "woot_zone"
	metricURI, _ := buildMetricURI(testString)
	if metricURI != "testomcpresto/v1/jmx/mbean/this_is_a_test" {
		t.Error("Expected testomcpresto/v1/jmx/mbean/this_is_a_test, got ", metricURI)
	}
}

func TestGetCoordinatorURIIsSetFromEnvironment(t *testing.T) {
	coordinatorURI := getCoordinatorURI()
	if coordinatorURI != "testomcpresto/v1/jmx/mbean/" {
		t.Error("Expected testomcpresto/v1/jmx/mbean/, got", coordinatorURI)
	}
}
