package presto_metrico

import (
	"encoding/json"
	"fmt"
	"github.com/ooyala/go-dogstatsd"
	"log"
	"net/http"
	"strconv"
)

var coordinatorUri string

const jmxSuffix string = "/v1/jmx/mbean/"

var (
	jmxBeans = map[string]string{
		"queryManager":         "com.facebook.presto.execution:name=QueryManager",
		"nodeScheduler":        "com.facebook.presto.execution:name=NodeScheduler",
		"taskExecutor":         "com.facebook.presto.execution:name=TaskExecutor",
		"taskManager":          "com.facebook.presto.execution:name=TaskManager",
		"memoryPoolGeneral":    "com.facebook.presto.memory:type=MemoryPool,name=general",
		"memoryPoolReserved":   "com.facebook.presto.memory:type=MemoryPool,name=reserved",
		"clusterMemoryManager": "com.facebook.presto.memory:name=ClusterMemoryManager",
	}

	datadogMetrics = map[string]string{
		"Executor.ActiveCount":      "queryManager",
		"Executor.QueuedTaskCount":  "queryManager",
		"Executor.TaskCount":        "queryManager",
		"StartedQueries.TotalCount": "queryManager",
		"ClusterMemoryBytes":        "clusterMemoryManager",
		"ClusterMemoryUsageBytes":   "clusterMemoryManager",
	}
)

type JMXMetricAttribute struct {
	Name  string
	Type  string
	Value interface{}
}

type JMXMetric struct {
	ClassName  string
	Attributes []JMXMetricAttribute `json:"attributes"`
}

func Configure(coordinator string) {
	coordinatorUri = fmt.Sprintf("%s%s", coordinator, jmxSuffix)
}

func buildMetricUri(metric string) string {
	return fmt.Sprintf("%s%s", coordinatorUri, jmxBeans[metric])
}

func getMetric(metricName string) (*JMXMetric, error) {
	uri := buildMetricUri(metricName)
	resp, err := http.Get(uri)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var jmxMetric JMXMetric
	err = decoder.Decode(&jmxMetric)
	return &jmxMetric, err
}

func convertJMXAttributeToString(metric interface{}) string {
	switch v := metric.(type) {
	case int:
		return strconv.Itoa(v)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	default:
		return metric.(string)
	}
}

func SendJMXMetrics(client *dogstatsd.Client) {
	for metricName, _ := range jmxBeans {
		metric, err := getMetric(metricName)

		if err != nil {
			log.Println(err)
		}

		for _, attribute := range metric.Attributes {
			_, ok := datadogMetrics[attribute.Name]
			if ok {
				datadogLabel := fmt.Sprintf("%s.%s", metricName, attribute.Name)
				client.Gauge(datadogLabel, attribute.Value.(float64), nil, 1.0)
			}
		}
	}
}
