package parsing

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/noxiouz/Combaine/combainer/slave"
	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/logger"

	"github.com/noxiouz/Combaine/common/tasks"
)

const (
	storageServiceName = "elliptics"
)

type ParsingContext struct {
	Ctx      context.Context
	Resolver slave.Resolver
}

func fetchDataFromTarget(task *tasks.ParsingTask) ([]byte, error) {
	fetcherType, err := task.ParsingConfig.DataFetcher.Type()
	if err != nil {
		return nil, err
	}

	logger.Debugf("%s use %s for fetching data", task.Id, fetcherType)
	fetcher, err := NewFetcher(fetcherType, task.ParsingConfig.DataFetcher)
	if err != nil {
		return nil, err
	}

	fetcherTask := tasks.FetcherTask{
		Target:     task.Host,
		CommonTask: task.CommonTask,
	}

	blob, err := fetcher.Fetch(&fetcherTask)
	if err != nil {
		return nil, err
	}

	logger.Debugf("%s Fetch %d bytes from %s: %s", task.Id, len(blob), task.Host, blob)
	return blob, nil
}

type ParsingResult struct {
	data map[string][]byte
	mu   sync.Mutex
}

func NewParsingResult() *ParsingResult {
	return &ParsingResult{
		data: make(map[string][]byte),
	}
}

func (pr *ParsingResult) Put(config, item string, result []byte) {
	pr.mu.Lock()
	pr.data[config+":"+item] = result
	pr.mu.Unlock()
}

func Parsing(ctx *ParsingContext, task tasks.ParsingTask) (*ParsingResult, error) {
	logger.Infof("%s start parsing", task.Id)

	var (
		blob    []byte
		err     error
		payload interface{}
		wg      sync.WaitGroup
	)

	blob, err = fetchDataFromTarget(&task)
	if err != nil {
		logger.Errf("%s error `%v` occured while fetching data", task.Id, err)
		return nil, err
	}

	if !task.ParsingConfig.SkipParsingStage() {
		logger.Infof("%s Send data to parsing", task.Id)

		parsingApp, err := ctx.Resolver.Resolve(ctx.Ctx, common.PARSINGAPP)
		if err != nil {
			logger.Errf("%s error `%v` occured while resolving %s",
				task.Id, err, common.PARSINGAPP)
			return nil, err
		}

		taskToParser, _ := common.Pack([]interface{}{task.Id, task.ParsingConfig.Parser, blob})
		if err := parsingApp.Do("enqueue", "parse", taskToParser).Wait(ctx.Ctx, &blob); err != nil {
			logger.Errf("%s error `%v` occured while parsing data", task.Id, err)
			return nil, err
		}
	}

	payload = blob

	if !task.ParsingConfig.Raw {
		logger.Debugf("%s Use %s for handle data", task.Id, common.DATABASEAPP)

		datagrid, err := ctx.Resolver.Resolve(ctx.Ctx, common.DATABASEAPP)
		if err != nil {
			logger.Errf("%s unable to get DG %v", task.Id, err)
			return nil, err
		}

		var token string
		if err := datagrid.Do("enqueue", "put", blob).Wait(ctx.Ctx, &token); err != nil {
			logger.Errf("%s unable to put data to DG %v", task.Id, err)
			return nil, err
		}

		defer func() {
			taskToDatagrid, _ := common.Pack([]interface{}{token})
			datagrid.Do("enqueue", "drop", taskToDatagrid)
			logger.Debugf("%s Drop table", task.Id)
		}()
		payload = token
	}

	pr := NewParsingResult()

	for aggLogName, aggCfg := range task.AggregationConfigs {
		for k, v := range aggCfg.Data {
			aggType, err := v.Type()
			if err != nil {
				logger.Errf("no type in configuration: %s %s %v", aggLogName, k, v)
				return nil, err
			}

			logger.Debugf("%s Send to %s %s type %s %v", task.Id, aggLogName, k, aggType, v)

			wg.Add(1)
			go func(name string, dataName string, v interface{}, configName string) {
				defer wg.Done()

				app, err := ctx.Resolver.Resolve(ctx.Ctx, name)
				if err != nil {
					logger.Errf("%s %s %s", task.Id, name, err)
					return
				}

				// Task structure
				t, _ := common.Pack(map[string]interface{}{
					"config":   v,
					"token":    payload,
					"prevtime": task.PrevTime,
					"currtime": task.CurrTime,
					"id":       task.Id,
				})

				var rawRes []byte
				if err := app.Do("enqueue", "aggregate_host", t).Wait(ctx.Ctx, &rawRes); err != nil {
					logger.Errf("%s Failed task: %v", task.Id, err)
					return
				}

				logger.Debugf("result for %s %s: %v", configName, dataName, rawRes)

				pr.Put(configName, dataName, rawRes)

			}(aggType, k, v, aggLogName)
		}
	}
	wg.Wait()

	logger.Infof("%s Done", task.Id)
	return pr, nil
}
