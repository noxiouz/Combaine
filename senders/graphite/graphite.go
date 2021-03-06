package graphite

import (
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/tasks"
)

func formatSubgroup(input string) string {
	return strings.Replace(
		strings.Replace(input, ".", "_", -1),
		"-", "_", -1)
}

const (
	onePointFormat = "%s.combaine.%s.%s %s %d\n"

	connectionTimeout  = 900      //msec
	connectionEndpoint = ":42000" //msec
)

type GraphiteSender interface {
	Send(tasks.DataType, uint64) error
}

type graphiteClient struct {
	id      string
	cluster string
	fields  []string
}

type GraphiteCfg struct {
	Cluster string   `codec:"cluster"`
	Fields  []string `codec:"Fields"`
}

type NameStack []string

func (n *NameStack) Push(item string) {
	*n = append(*n, item)
}

func (n *NameStack) Pop() (item string) {
	item, *n = (*n)[len(*n)-1], (*n)[:len(*n)-1]
	return item
}

type pointFormat func(NameStack, interface{}, uint64) string

func makePoint(format, cluster, subgroup string) pointFormat {
	return func(metric NameStack, value interface{}, timestamp uint64) string {
		return fmt.Sprintf(
			format,
			cluster,
			formatSubgroup(subgroup),
			strings.Join(metric, "."),
			common.InterfaceToString(value),
			timestamp,
		)
	}
}

func (g *graphiteClient) send(output io.Writer, data string) error {
	logger.Infof("%s Send %s", g.id, data)
	if _, err := fmt.Fprint(output, data); err != nil {
		logger.Errf("%s Sending error: %s", g.id, err)
		return err
	}
	return nil
}

func (g *graphiteClient) sendInterface(output io.Writer, metricName NameStack,
	f pointFormat, value interface{}, timestamp uint64) error {
	data := f(metricName, value, timestamp)
	return g.send(output, data)
}

func (g *graphiteClient) sendSlice(output io.Writer, metricName NameStack, f pointFormat,
	rv reflect.Value, timestamp uint64) error {

	if len(g.fields) == 0 || len(g.fields) != rv.Len() {
		logger.Errf("%s Unable to send a slice. Fields len %d, len of value %d", g.id, len(g.fields), rv.Len())
		val := make([]int, len(g.fields))
		for i := range g.fields {
			val[i] = 1
		}
		rv = reflect.ValueOf(val)
	}

	for i := 0; i < rv.Len(); i++ {
		metricName.Push(g.fields[i])

		item := rv.Index(i).Interface()
		err := g.sendInterface(output, metricName, f, common.InterfaceToString(item), timestamp)
		if err != nil {
			return err
		}

		metricName.Pop()
	}

	return nil
}

func (g *graphiteClient) sendMap(output io.Writer, metricName NameStack, f pointFormat,
	rv reflect.Value, timestamp uint64) (err error) {

	keys := rv.MapKeys()
	for _, key := range keys {
		// Push key of map
		metricName.Push(common.InterfaceToString(key.Interface()))

		itemInterface := reflect.ValueOf(rv.MapIndex(key).Interface())
		logger.Debugf("%s Item of key %s is: %v", g.id, key, itemInterface.Kind())

		switch itemInterface.Kind() {
		case reflect.Slice, reflect.Array:
			err = g.sendSlice(output, metricName, f, itemInterface, timestamp)
			if err != nil {
				return err
			}

		case reflect.Map:
			err = g.sendMap(output, metricName, f, itemInterface, timestamp)
			if err != nil {
				return err
			}

		default:
			err = g.sendInterface(output, metricName, f,
				common.InterfaceToString(itemInterface.Interface()), timestamp)
			if err != nil {
				return err
			}
		}

		// Pop key of map
		metricName.Pop()
	}

	return nil
}

func (g *graphiteClient) sendInternal(data *tasks.DataType, timestamp uint64, output io.Writer) error {
	var err error
	metricName := make(NameStack, 0, 3)

	for aggname, subgroupsAndValues := range *data {
		logger.Debugf("%s Handle aggregate named %s", g.id, aggname)

		metricName.Push(aggname)
		for subgroup, value := range subgroupsAndValues {
			pointFormatter := makePoint(onePointFormat, g.cluster, subgroup)
			rv := reflect.ValueOf(value)
			logger.Debugf("%s %s", g.id, rv.Kind())

			switch rv.Kind() {
			case reflect.Slice, reflect.Array:
				err = g.sendSlice(output, metricName, pointFormatter, rv, timestamp)

			case reflect.Map:
				err = g.sendMap(output, metricName, pointFormatter, rv, timestamp)

			default:
				err = g.sendInterface(output, metricName, pointFormatter, common.InterfaceToString(value), timestamp)

			}
			if err != nil {
				return err
			}
		}
		metricName.Pop()
	}
	return nil
}

func (g *graphiteClient) Send(data tasks.DataType, timestamp uint64) (err error) {
	if len(data) == 0 {
		return fmt.Errorf("%s Empty data. Nothing to send.", g.id)
	}

	sock, err := net.DialTimeout("tcp", connectionEndpoint, time.Microsecond*connectionTimeout)
	if err != nil {
		logger.Errf("Unable to connect to daemon %s: %s", connectionEndpoint, err)
		return
	}
	defer sock.Close()
	return g.sendInternal(&data, timestamp, sock)
}

func NewGraphiteClient(cfg *GraphiteCfg, id string) (gs GraphiteSender, err error) {
	gs = &graphiteClient{
		id:      id,
		cluster: cfg.Cluster,
		fields:  cfg.Fields,
	}

	return
}
