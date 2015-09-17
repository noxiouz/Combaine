package aggregation

import (
	"golang.org/x/net/context"

	"github.com/noxiouz/Combaine/common/logger"
	"github.com/noxiouz/Combaine/common/tasks"
)

type AggregationContext struct {
	Ctx      context.Context
	Resolver slave.Resolver
}

func Aggregate(ctx *AggregationContext, task tasks.AggregationTask) error {
	logger.Debugf("%s Start aggregation", task.Id)
	return nil
}
