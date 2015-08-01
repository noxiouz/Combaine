package parsing

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"

	"github.com/noxiouz/Combaine/combainer/slave"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/tasks"
)

type mockSlave struct {
	res interface{}
	err error
}

func (m *mockSlave) Close() {}

func (m *mockSlave) Endpoint() string {
	return "localhost:10053"
}

func (m *mockSlave) Do(name string, args ...interface{}) slave.AsyncResult {
	return m
}

func (m *mockSlave) Wait(ctx context.Context, result interface{}) error {
	if m.err != nil {
		return m.err
	}

	v := reflect.ValueOf(result)
	res := reflect.ValueOf(m.res)
	reflect.Indirect(v).Set(res)
	return nil
}

func (m *mockSlave) Resolve(ctx context.Context, name string) (slave.Slave, error) {
	return m, nil
}

var task = tasks.ParsingTask{
	CommonTask: tasks.CommonTask{
		Id:       "id",
		PrevTime: time.Now().Unix() - 10,
		CurrTime: time.Now().Unix(),
	},
	Host:              "target1",
	ParsingConfigName: "parsing",
	ParsingConfig: configs.ParsingConfig{
		Groups:      []string{"G1"},
		AggConfigs:  []string{"A", "B"},
		Parser:      "NullParser",
		Metahost:    "MetaHost",
		Raw:         true,
		DataFetcher: configs.PluginConfig{},
	},
	AggregationConfigs: map[string]configs.AggregationConfig{
		"A": configs.AggregationConfig{
			Data: map[string]configs.PluginConfig{
				"200x": configs.PluginConfig{},
			},
			Senders: map[string]configs.PluginConfig{},
		},
	},
}

func TestParsingNoFetcher(t *testing.T) {
	ctxParsing := &ParsingContext{
		Ctx:      context.Background(),
		Resolver: slave.NewLocalResolver(),
	}

	tt := task
	tt.ParsingConfig.DataFetcher["type"] = "AA"
	_, err := Parsing(ctxParsing, tt)
	assert.EqualError(t, err, "Fetcher AA isn't available")
}

func TestParsingNoData(t *testing.T) {
	Register("mock", newMockFetcher)
	defer unregister("mock")

	ctxParsing := &ParsingContext{
		Ctx:      context.Background(),
		Resolver: slave.NewLocalResolver(),
	}

	tt := task
	tt.ParsingConfig.DataFetcher["type"] = "mock"
	_, err := Parsing(ctxParsing, tt)
	assert.EqualError(t, err, "No data fetcher error")
}

func TestParsingMissingType(t *testing.T) {
	Register("mock", newMockFetcher)
	defer unregister("mock")

	ctxParsing := &ParsingContext{
		Ctx:      context.Background(),
		Resolver: slave.NewLocalResolver(),
	}

	tt := task
	tt.ParsingConfig.DataFetcher["type"] = "mock"
	tt.ParsingConfig.DataFetcher["data"] = []byte("data: 1")
	_, err := Parsing(ctxParsing, tt)
	assert.EqualError(t, err, "Missing `type` value")
}

func TestParsing(t *testing.T) {
	Register("mock", newMockFetcher)
	defer unregister("mock")

	ctxParsing := &ParsingContext{
		Ctx: context.Background(),
		Resolver: &mockSlave{
			res: []byte("1"),
		},
	}

	tt := task
	tt.ParsingConfig.DataFetcher["type"] = "mock"
	tt.ParsingConfig.DataFetcher["data"] = []byte("data: 1")

	tt.AggregationConfigs = map[string]configs.AggregationConfig{
		"A": configs.AggregationConfig{
			Data: map[string]configs.PluginConfig{
				"200x": configs.PluginConfig{
					"type": "mock",
				},
				"300x": configs.PluginConfig{
					"type": "mock",
				},
			},
			Senders: map[string]configs.PluginConfig{},
		},
	}

	res, err := Parsing(ctxParsing, tt)
	assert.NoError(t, err)
	assert.Equal(t, []byte("1"), res.data["A:200x"])
	assert.Equal(t, []byte("1"), res.data["A:300x"])
}
