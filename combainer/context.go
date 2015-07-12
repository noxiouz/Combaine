package combainer

import (
	"github.com/Sirupsen/logrus"

	"github.com/noxiouz/Combaine/combainer/discovery"
	"github.com/noxiouz/Combaine/combainer/slave"
	"github.com/noxiouz/Combaine/common/cache"
)

type Context struct {
	*logrus.Logger
	Cache     cache.Cache
	Discovery discovery.Discovery
	Resolver  slave.Resolver
}
