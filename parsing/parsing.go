package parsing

import (
	"fmt"
	"github.com/noxiouz/Combaine/combainer/slave"
	"sync"
	"time"

	"golang.org/x/net/context"

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

func Parsing(ctx *ParsingContext, task tasks.ParsingTask) error {
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
		return err
	}

	if !task.ParsingConfig.SkipParsingStage() {
		logger.Infof("%s Send data to parsing", task.Id)

		parsingApp, err := ctx.Resolver.Resolve(ctx.Ctx, common.PARSINGAPP)
		if err != nil {
			logger.Errf("%s error `%v` occured while resolving %s",
				task.Id, err, common.PARSINGAPP)
			return err
		}

		taskToParser, _ := common.Pack([]interface{}{task.Id, task.ParsingConfig.Parser, blob})
		if err := parsingApp.Do("enqueue", "parse", taskToParser).Wait(ctx.Ctx, &blob); err != nil {
			logger.Errf("%s error `%v` occured while parsing data", task.Id, err)
			return err
		}
	}

	payload = blob

	if !task.ParsingConfig.Raw {
		logger.Debugf("%s Use %s for handle data", task.Id, common.DATABASEAPP)

		datagrid, err := ctx.Resolver.Resolve(ctx.Ctx, common.DATABASEAPP)
		if err != nil {
			logger.Errf("%s unable to get DG %v", task.Id, err)
			return err
		}

		var token string
		if err := datagrid.Do("enqueue", "put", blob).Wait(ctx.Ctx, &token); err != nil {
			logger.Errf("%s unable to put data to DG %v", task.Id, err)
			return err
		}

		defer func() {
			taskToDatagrid, _ := common.Pack([]interface{}{token})
			datagrid.Do("enqueue", "drop", taskToDatagrid)
			logger.Debugf("%s Drop table", task.Id)
		}()
		payload = token
	}

	for aggLogName, aggCfg := range task.AggregationConfigs {
		for k, v := range aggCfg.Data {
			aggType, err := v.Type()
			if err != nil {
				return err
			}

			logger.Debugf("%s Send to %s %s type %s %v", task.Id, aggLogName, k, aggType, v)

			wg.Add(1)
			go func(name string, k string, v interface{}, logName string, deadline time.Duration) {
				defer wg.Done()
				app, err := ctx.Resolver.Resolve(ctx.Ctx, name)
				if err != nil {
					logger.Errf("%s %s %s", task.Id, name, err)
					return
				}

				/*
					Task structure
				*/
				t, _ := common.Pack(map[string]interface{}{
					"config":   v,
					"token":    payload,
					"prevtime": task.PrevTime,
					"currtime": task.CurrTime,
					"id":       task.Id,
				})

				var rawRes []byte
				aggHostCtx, aggHostCtxCancel := context.WithTimeout(ctx.Ctx, deadline)
				defer aggHostCtxCancel()
				if err := app.Do("enqueue", "aggregate_host", t).Wait(aggHostCtx, &rawRes); err != nil {
					logger.Errf("%s Failed task: %v", task.Id, err)
					return
				}

				key := fmt.Sprintf("%s;%s;%s;%s;%v", task.Host, task.ParsingConfigName,
					logName, k, task.CurrTime)

				storage, err := ctx.Resolver.Resolve(aggHostCtx, storageServiceName)
				if err != nil {
					logger.Errf("%s Failed to get storage: %v", task.Id, err)
					return
				}

				storage.Do("cache_write", "combaine", key, rawRes)
				logger.Debugf("%s Write data with key %s", task.Id, key)

			}(aggType, k, v, aggLogName, time.Second*time.Duration(task.CurrTime-task.PrevTime))
		}
	}
	wg.Wait()

	logger.Infof("%s Done", task.Id)
	return nil
}
