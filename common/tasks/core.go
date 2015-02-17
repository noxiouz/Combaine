package tasks

import (
	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/hosts"
)

type Task interface {
	Id() string
	Raw() ([]byte, error)
	Group() string
}

type CommonTask struct {
	Id       string `codec:"Id"`
	PrevTime int64  `codec:"PrevTime"`
	CurrTime int64  `codec:"CurrTime"`
}

var (
	EmptyCommonTask = CommonTask{
		Id:       "",
		PrevTime: -1,
		CurrTime: -1}
)

type ParsingTask struct {
	CommonTask
	// Hostname of target
	Host string
	// Name of handled parsing config
	ParsingConfigName string
	// Content of the current parsing config
	ParsingConfig configs.ParsingConfig
	// Content of aggreagtion configs
	// related to the current parsing config
	AggregationConfigs map[string]configs.AggregationConfig
}

func (p *ParsingTask) Id() string {
	return p.CommonTask.Id
}

func (p *ParsingTask) Group() string {
	return p.ParsingConfig.GetGroup()
}

func (p *ParsingTask) Raw() ([]byte, error) {
	return common.Pack(p)
}

type AggregationTask struct {
	CommonTask
	// Name of the current aggregation config
	Config string
	// Name of handled parsing config
	ParsingConfigName string
	// Content of the current parsing config
	ParsingConfig configs.ParsingConfig
	// Current aggregation config
	AggregationConfig configs.AggregationConfig
	// Hosts
	Hosts hosts.Hosts
}

func (a *AggregationTask) Id() string {
	return a.CommonTask.Id
}

func (a *AggregationTask) Group() string {
	return a.ParsingConfig.GetGroup()
}

func (a *AggregationTask) Raw() ([]byte, error) {
	return common.Pack(a)
}
