package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/ooyala/go-dogstatsd"
)

var (
	jmxSuffix   = "/v1/jmx/mbean/"
	coordinator string // Global variable set via environment variable
	jmxBeans    = map[string]string{
		"queryManager":         "com.facebook.presto.execution:name=QueryManager",
		"taskExecutor":         "com.facebook.presto.execution:name=TaskExecutor",
		"taskManager":          "com.facebook.presto.execution:name=TaskManager",
		"memoryPoolGeneral":    "com.facebook.presto.memory:type=MemoryPool,name=general",
		"clusterMemoryManager": "com.facebook.presto.memory:name=ClusterMemoryManager",
	}

	datadogMetrics = map[string]string{
		"Executor.ActiveCount":                              "queryManager",
		"Executor.QueuedTaskCount":                          "queryManager",
		"Executor.TaskCount":                                "queryManager",
		"Executor.CompletedTaskCount":                       "queryManager",
		"Executor.CorePoolSize":                             "queryManager",
		"Executor.PoolSize":                                 "queryManager",
		"ManagementExecutor.ActiveCount":                    "queryManager",
		"ManagementExecutor.CompletedTaskCount":             "queryManager",
		"ManagementExecutor.QueuedTaskCount":                "queryManager",
		"AbandonedQueries.FifteenMinute.Count":              "queryManager",
		"AbandonedQueries.FifteenMinute.Rate":               "queryManager",
		"AbandonedQueries.FiveMinute.Count":                 "queryManager",
		"AbandonedQueries.FiveMinute.Rate":                  "queryManager",
		"AbandonedQueries.OneMinute.Count":                  "queryManager",
		"AbandonedQueries.OneMinute.Rate":                   "queryManager",
		"AbandonedQueries.TotalCount":                       "queryManager",
		"CanceledQueries.FifteenMinute.Count":               "queryManager",
		"CanceledQueries.FifteenMinute.Rate":                "queryManager",
		"CanceledQueries.FiveMinute.Count":                  "queryManager",
		"CanceledQueries.FiveMinute.Rate":                   "queryManager",
		"CanceledQueries.OneMinute.Count":                   "queryManager",
		"CanceledQueries.OneMinute.Rate":                    "queryManager",
		"CanceledQueries.TotalCount":                        "queryManager",
		"CompletedQueries.FifteenMinute.Count":              "queryManager",
		"CompletedQueries.FifteenMinute.Rate":               "queryManager",
		"CompletedQueries.FiveMinute.Count":                 "queryManager",
		"CompletedQueries.FiveMinute.Rate":                  "queryManager",
		"CompletedQueries.OneMinute.Count":                  "queryManager",
		"CompletedQueries.OneMinute.Rate":                   "queryManager",
		"CompletedQueries.TotalCount":                       "queryManager",
		"CpuInputByteRate.AllTime.P95":                      "queryManager",
		"CpuInputByteRate.FifteenMinutes.P95":               "queryManager",
		"CpuInputByteRate.FiveMinutes.P95":                  "queryManager",
		"CpuInputByteRate.OneMinute.P95":                    "queryManager",
		"ExecutionTime.AllTime.P95":                         "queryManager",
		"ExecutionTime.FifteenMinutes.P95":                  "queryManager",
		"ExecutionTime.FiveMinutes.P95":                     "queryManager",
		"ExecutionTime.OneMinute.P95":                       "queryManager",
		"FailedQueries.FifteenMinute.Count":                 "queryManager",
		"FailedQueries.FifteenMinute.Rate":                  "queryManager",
		"FailedQueries.FiveMinute.Count":                    "queryManager",
		"FailedQueries.FiveMinute.Rate":                     "queryManager",
		"FailedQueries.OneMinute.Count":                     "queryManager",
		"FailedQueries.OneMinute.Rate":                      "queryManager",
		"FailedQueries.TotalCount":                          "queryManager",
		"InsufficientResourcesFailures.FifteenMinute.Count": "queryManager",
		"InsufficientResourcesFailures.FifteenMinute.Rate":  "queryManager",
		"InsufficientResourcesFailures.FiveMinute.Count":    "queryManager",
		"InsufficientResourcesFailures.FiveMinute.Rate":     "queryManager",
		"InsufficientResourcesFailures.OneMinute.Count":     "queryManager",
		"InsufficientResourcesFailures.OneMinute.Rate":      "queryManager",
		"InsufficientResourcesFailures.TotalCount":          "queryManager",
		"InternalFailures.FifteenMinute.Count":              "queryManager",
		"InternalFailures.FifteenMinute.Rate":               "queryManager",
		"InternalFailures.FiveMinute.Count":                 "queryManager",
		"InternalFailures.FiveMinute.Rate":                  "queryManager",
		"InternalFailures.OneMinute.Count":                  "queryManager",
		"InternalFailures.OneMinute.Rate":                   "queryManager",
		"InternalFailures.TotalCount":                       "queryManager",
		"RunningQueries":                                    "queryManager",
		"StartedQueries.FifteenMinute.Count":                "queryManager",
		"StartedQueries.FifteenMinute.Rate":                 "queryManager",
		"StartedQueries.FiveMinute.Count":                   "queryManager",
		"StartedQueries.FiveMinute.Rate":                    "queryManager",
		"StartedQueries.OneMinute.Count":                    "queryManager",
		"StartedQueries.OneMinute.Rate":                     "queryManager",
		"StartedQueries.TotalCount":                         "queryManager",
		"UserErrorFailures.FifteenMinute.Count":             "queryManager",
		"UserErrorFailures.FifteenMinute.Rate":              "queryManager",
		"UserErrorFailures.FiveMinute.Count":                "queryManager",
		"UserErrorFailures.FiveMinute.Rate":                 "queryManager",
		"UserErrorFailures.OneMinute.Count":                 "queryManager",
		"UserErrorFailures.OneMinute.Rate":                  "queryManager",
		"UserErrorFailures.TotalCount":                      "queryManager",
		"ProcessorExecutor.QueuedTaskCount":                 "taskExecutor",
		"BlockedSplits":                                     "taskExecutor",
		"PendingSplits":                                     "taskExecutor",
		"RunningSplits":                                     "taskExecutor",
		"QueuedTime.FifteenMinutes.P95":                     "taskExecutor",
		"QueuedTime.FiveMinutes.P95":                        "taskExecutor",
		"QueuedTime.OneMinute.P95":                          "taskExecutor",
		"InputDataSize.FifteenMinute.Count":                 "taskManager",
		"InputDataSize.FifteenMinute.Rate":                  "taskManager",
		"InputDataSize.FiveMinute.Count":                    "taskManager",
		"InputDataSize.FiveMinute.Rate":                     "taskManager",
		"InputDataSize.OneMinute.Count":                     "taskManager",
		"InputDataSize.OneMinute.Rate":                      "taskManager",
		"InputPositions.FifteenMinute.Count":                "taskManager",
		"InputPositions.FifteenMinute.Rate":                 "taskManager",
		"InputPositions.FiveMinute.Count":                   "taskManager",
		"InputPositions.FiveMinute.Rate":                    "taskManager",
		"InputPositions.OneMinute.Count":                    "taskManager",
		"InputPositions.OneMinute.Rate":                     "taskManager",
		"OutputDataSize.FifteenMinute.Count":                "taskManager",
		"OutputDataSize.FifteenMinute.Rate":                 "taskManager",
		"OutputDataSize.FiveMinute.Count":                   "taskManager",
		"OutputDataSize.FiveMinute.Rate":                    "taskManager",
		"OutputDataSize.OneMinute.Count":                    "taskManager",
		"OutputDataSize.OneMinute.Rate":                     "taskManager",
		"OutputPositions.FifteenMinute.Count":               "taskManager",
		"OutputPositions.FifteenMinute.Rate":                "taskManager",
		"OutputPositions.FiveMinute.Count":                  "taskManager",
		"OutputPositions.FiveMinute.Rate":                   "taskManager",
		"OutputPositions.OneMinute.Count":                   "taskManager",
		"OutputPositions.OneMinute.Rate":                    "taskManager",
		"TaskManagementExecutor.PoolSize":                   "taskManager",
		"TaskManagementExecutor.QueuedTaskCount":            "taskManager",
		"TaskManagementExecutor.TaskCount":                  "taskManager",
		"TaskNotificationExecutor.ActiveCount":              "taskManager",
		"TaskNotificationExecutor.PoolSize":                 "taskManager",
		"TaskNotificationExecutor.QueuedTaskCount":          "taskManager",
		"FreeBytes":                                         "memoryPoolGeneral",
		"MaxBytes":                                          "memoryPoolGeneral",
		"ClusterMemoryBytes":                                "clusterMemoryManager",
		"ClusterMemoryUsageBytes":                           "clusterMemoryManager",
	}
)

// JMXMetricAttribute represents the jmx attribute containing information about
// a specific attribute of a jmx metric
type JMXMetricAttribute struct {
	Name  string
	Value interface{}
}

// JMXMetric represents the top level jmx metric.
type JMXMetric struct {
	ClassName  string
	Attributes []JMXMetricAttribute `json:"attributes"`
}

func setCoordinatorFromEnvironment() string {
	if coordinator == "" {
		coordinator = os.Getenv("PRESTO_COORDINATOR")
	}
	return coordinator
}

func getCoordinatorURI() string {
	return fmt.Sprintf("%s%s", setCoordinatorFromEnvironment(), jmxSuffix)
}

func buildMetricURI(metric string) (string, error) {
	jmxString, ok := jmxBeans[metric]
	if !ok {
		return "", errors.New("Metric string was not found for metric")
	}

	msg := fmt.Sprintf("%s%s", getCoordinatorURI(), jmxString)
	return msg, nil
}

func getHTTPRawResponse(uri string) (*http.Response, error) {
	resp, err := http.Get(uri)
	if err != nil {
		return nil, err
	}
	return resp, err
}

func retriveRawMetricResponse(metricName string) (*http.Response, error) {
	uri, err := buildMetricURI(metricName)

	if err != nil {
		return nil, err
	}

	resp, err := getHTTPRawResponse(uri)
	return resp, err
}

func decodeRawMetricResponse(resp *http.Response) (*JMXMetric, error) {
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var jmxMetric JMXMetric
	err := decoder.Decode(&jmxMetric)
	return &jmxMetric, err
}

func getMetric(metricName string) (*JMXMetric, error) {
	resp, err := retriveRawMetricResponse(metricName)

	if err != nil {
		return nil, err
	}

	return decodeRawMetricResponse(resp)
}

func sendJMXMetric(client *dogstatsd.Client, metricCatagory string, attribute JMXMetricAttribute) {
	_, ok := datadogMetrics[attribute.Name]
	if ok {
		switch val := attribute.Value.(type) {
		case float64:
			datadogLabel := fmt.Sprintf("data.presto.%s.%s", metricCatagory, attribute.Name)
			client.Gauge(datadogLabel, val, nil, 1.0)
		default:
			log.Println("skipping attribute %q: cannot handle value %v type %T", attribute.Name, val, val)
		}
	}
}

// ProcessJMXMetrics retrieves and processes metrics from the presto coordinator
// sending them to datadog server
func ProcessJMXMetrics(client *dogstatsd.Client) {
	for metricName := range jmxBeans {
		metric, err := getMetric(metricName)

		if err != nil {
			log.Printf("getMetric(%q): %v", metricName, err)
			continue
		}

		for _, attribute := range metric.Attributes {
			sendJMXMetric(client, metricName, attribute)
		}
	}
}
