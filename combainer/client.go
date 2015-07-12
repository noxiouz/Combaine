package combainer

import (
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"
	"github.com/noxiouz/Combaine/common/hosts"
	"github.com/noxiouz/Combaine/common/tasks"
)

var (
	ErrHandlerTimeout = fmt.Errorf("Timeout")
)

type sessionParams struct {
	ParsingTime time.Duration
	WholeTime   time.Duration
	PTasks      []tasks.ParsingTask
	AggTasks    []tasks.AggregationTask
}

type Client struct {
	*Context
	*Stats
	Id         string
	Repository configs.Repository
	Log        *logrus.Entry
	context    context.Context
}

func NewClient(ctx *Context, repo configs.Repository) (*Client, error) {
	id := GenerateSessionId()
	cl := &Client{
		Context: ctx,
		Stats:   NewStats(),

		Id:         id,
		Repository: repo,
		Log:        ctx.Logger.WithField("client", id),
		context:    context.Background(),
	}

	return cl, nil
}

func (cl *Client) UpdateSessionParams(config string) (sp *sessionParams, err error) {
	startTime := time.Now()
	defer cl.Stats.timingPreparing.UpdateSince(startTime)

	cl.Log.WithFields(logrus.Fields{
		"config": config,
	}).Info("updating session parametrs")

	var (
		// tasks
		pTasks   []tasks.ParsingTask
		aggTasks []tasks.AggregationTask

		// timeouts
		parsingTime time.Duration
		wholeTime   time.Duration
	)

	encodedParsingConfig, err := cl.Repository.GetParsingConfig(config)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"config": config,
			"error":  err,
		}).Error("unable to load config")
		return nil, err
	}

	var parsingConfig configs.ParsingConfig
	if err := encodedParsingConfig.Decode(&parsingConfig); err != nil {
		cl.Log.WithFields(logrus.Fields{
			"config": config,
			"error":  err,
		}).Error("unable to decode parsingConfig")
		return nil, err
	}

	cfg := cl.Repository.GetCombainerConfig()
	parsingConfig.UpdateByCombainerConfig(&cfg)
	aggregationConfigs, err := GetAggregationConfigs(cl.Repository, &parsingConfig)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"config": config,
			"error":  err,
		}).Error("unable to read aggregation configs")
		return nil, err
	}

	cl.Log.Infof("updating config: group %s, metahost %s",
		parsingConfig.GetGroup(), parsingConfig.GetMetahost())

	hostFetcher, err := LoadHostFetcher(cl.Context, parsingConfig.HostFetcher)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"config": config,
			"error":  err,
		}).Error("Unable to construct SimpleFetcher")
		return
	}

	allHosts := make(hosts.Hosts)
	for _, item := range parsingConfig.Groups {
		hosts_for_group, err := hostFetcher.Fetch(item)
		if err != nil {
			cl.Log.WithFields(logrus.Fields{
				"config": config,
				"error":  err,
				"group":  item,
			}).Warn("unable to get hosts")
			continue
		}

		allHosts.Merge(&hosts_for_group)
	}

	listOfHosts := allHosts.AllHosts()

	if len(listOfHosts) == 0 {
		err := fmt.Errorf("No hosts in given groups")
		cl.Log.WithFields(logrus.Fields{
			"config": config,
			"group":  parsingConfig.Groups,
		}).Warn("no hosts in given groups")
		return nil, err
	}

	cl.Log.WithFields(logrus.Fields{
		"config": config,
	}).Infof("hosts: %s", listOfHosts)

	// Tasks for parsing
	for _, host := range listOfHosts {
		pTasks = append(pTasks, tasks.ParsingTask{
			CommonTask:         tasks.EmptyCommonTask,
			Host:               host,
			ParsingConfigName:  config,
			ParsingConfig:      parsingConfig,
			AggregationConfigs: *aggregationConfigs,
		})
	}

	for _, name := range parsingConfig.AggConfigs {
		aggTasks = append(aggTasks, tasks.AggregationTask{
			CommonTask:        tasks.EmptyCommonTask,
			Config:            name,
			ParsingConfigName: config,
			ParsingConfig:     parsingConfig,
			AggregationConfig: (*aggregationConfigs)[name],
			Hosts:             allHosts,
		})
	}

	parsingTime, wholeTime = GenerateSessionTimeFrame(parsingConfig.IterationDuration)

	sp = &sessionParams{
		ParsingTime: parsingTime,
		WholeTime:   wholeTime,
		PTasks:      pTasks,
		AggTasks:    aggTasks,
	}

	cl.Log.WithFields(logrus.Fields{
		"config": config,
	}).Infof("Session parametrs have been updated successfully. %v", sp)
	return sp, nil
}

func (cl *Client) Dispatch(parsingConfigName string, uniqueID string, shouldWait bool) error {
	cl.Stats.sessions.Inc(1)

	if uniqueID == "" {
		uniqueID = GenerateSessionId()
	}

	contextFields := logrus.Fields{
		"session": uniqueID,
		"config":  parsingConfigName}

	var (
		deadline time.Time
		wg       sync.WaitGroup

		startTime = time.Now()
	)

	sessionParameters, err := cl.UpdateSessionParams(parsingConfigName)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"session": uniqueID,
			"config":  parsingConfigName,
			"error":   err,
		}).Error("unable to update session parametrs")
		return err
	}

	deadline = startTime.Add(sessionParameters.ParsingTime)
	cl.Log.WithFields(contextFields).Info("Start new iteration")

	// Parsing phase
	totalTasksAmount := len(sessionParameters.PTasks)
	parsingCtx, parsingCancel := context.WithDeadline(cl.context, deadline)
	for i, task := range sessionParameters.PTasks {
		// Description of task
		task.PrevTime = startTime.Unix()
		task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
		task.CommonTask.Id = uniqueID

		cl.Log.WithFields(contextFields).Infof("Send task number %d/%d to parsing %v", i+1, totalTasksAmount, task)

		wg.Add(1)
		go func() {
			defer wg.Done()
			cl.doParsingTask(parsingCtx, task)
		}()
	}
	wg.Wait()
	// release all resources connected with context
	parsingCancel()

	cl.Log.WithFields(contextFields).Info("Parsing finished")

	// Aggregation phase
	deadline = startTime.Add(sessionParameters.WholeTime)
	totalTasksAmount = len(sessionParameters.AggTasks)
	aggContext, aggCancel := context.WithDeadline(cl.context, deadline)

	for i, task := range sessionParameters.AggTasks {
		task.PrevTime = startTime.Unix()
		task.CurrTime = startTime.Add(sessionParameters.WholeTime).Unix()
		task.CommonTask.Id = uniqueID

		cl.Log.WithFields(contextFields).Infof("Send task number %d/%d to aggregate %v", i+1, totalTasksAmount, task)

		wg.Add(1)
		go func() {
			defer wg.Done()
			cl.doAggregationHandler(aggContext, task)
		}()
	}
	wg.Wait()
	// release all resources connected with context
	aggCancel()

	cl.Log.WithFields(contextFields).Info("Aggregation has finished")

	// Wait for next iteration
	if shouldWait {
		idleTime := deadline.Sub(time.Now())
		cl.Stats.timingIdle.Update(idleTime)
		time.Sleep(idleTime)
	}

	cl.Log.WithFields(contextFields).Debug("Go to the next iteration")

	return nil
}

func (cl *Client) doGeneralTask(ctx context.Context, appName string, task tasks.Task) error {
	_, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("no deadline is set")
	}
	ctx = context.WithValue(ctx, "session", task.Tid())

	slave, err := cl.Resolver.Resolve(ctx, appName)
	if err != nil {
		cl.Log.WithFields(logrus.Fields{
			"session": task.Tid(),
			"error":   err,
			"appname": appName,
		}).Error("unable to send task")
		return err
	}

	raw, _ := task.Raw()
	var res tasks.TaskResult
	if err := slave.Do("enqueue", "handleTask", raw).Wait(ctx, &res); err != nil {
		cl.Log.WithFields(logrus.Fields{
			"session": task.Tid(),
			"error":   err,
			"appname": appName,
			"host":    slave.Endpoint(),
		}).Errorf("task for group %s failed", task.Group())
		return err
	}

	cl.Log.WithFields(logrus.Fields{
		"session": task.Tid(),
		"appname": appName,
		"host":    slave.Endpoint(),
	}).Infof("task for group %s done: %s", task.Group(), res)
	return nil
}

func (cl *Client) doParsingTask(ctx context.Context, task tasks.ParsingTask) {
	start := time.Now()
	defer cl.Stats.timingParsing.UpdateSince(start)

	if err := cl.doGeneralTask(ctx, common.PARSING, &task); err != nil {
		cl.AddFailedParsing()
		return
	}
	cl.AddSuccessParsing()
}

func (cl *Client) doAggregationHandler(ctx context.Context, task tasks.AggregationTask) {
	start := time.Now()
	defer cl.Stats.timingAggregate.UpdateSince(start)

	if err := cl.doGeneralTask(ctx, common.AGGREGATE, &task); err != nil {
		cl.AddFailedAggregate()
		return
	}
	cl.AddSuccessAggregate()
}
