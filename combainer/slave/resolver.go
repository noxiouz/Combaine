package slave

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/cocaine/cocaine-framework-go/cocaine"
	"golang.org/x/net/context"

	"github.com/noxiouz/Combaine/combainer/discovery"
)

var (
	ErrNoCloudHosts     = errors.New("no cloud hosts")
	ErrResolvingTimeout = errors.New("timed out")
	ErrAppUnavailable   = errors.New("application is unavailable")
)

type Resolver interface {
	Resolve(ctx context.Context, name string) (Slave, error)
	Close()
}

type cocaineResolver struct {
	mu        sync.Mutex
	hosts     []string
	discovery discovery.Discovery
	onClose   chan struct{}
	log       *logrus.Entry
}

func NewCocaineResolver(d discovery.Discovery) Resolver {
	cr := &cocaineResolver{
		discovery: d,
		hosts:     nil,
		onClose:   make(chan struct{}),
		log:       logrus.WithField("source", "discovery"),
	}

	cr.update()
	go cr.watcher()
	return cr
}

func (c *cocaineResolver) update() error {
	hosts, err := c.discovery.GetHosts()
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.hosts = hosts
	c.mu.Unlock()
	c.log.Info("hosts are updated successfully")
	return nil
}

func (c *cocaineResolver) watcher() {
	for {
		select {
		case <-time.After(10 * time.Second):
			if err := c.update(); err != nil {
				c.log.WithField("error", err).Error("unable to update hosts")
			}
		case <-c.onClose:
			return

		}
	}
}

type resolveInfo struct {
	slave Slave
	err   error
}

func resolve(appname, endpoint string) <-chan resolveInfo {
	res := make(chan resolveInfo)
	go func() {
		app, err := cocaine.NewService(appname, endpoint)
		select {
		case res <- resolveInfo{
			slave: NewSlave(app),
			err:   err,
		}:
		default:
			if err == nil {
				app.Close()
			}
		}
	}()
	return res
}

func (c *cocaineResolver) Resolve(ctx context.Context, name string) (Slave, error) {
	session := ctx.Value("session")
	var host string

	for {
		c.mu.Lock()
		if len(c.hosts) == 0 {
			return nil, ErrNoCloudHosts
		}
		host = getRandomHost(c.hosts) + ":10053"
		c.mu.Unlock()

		select {
		case r := <-resolve(name, host):
			if r.err != nil {
				c.log.WithFields(logrus.Fields{
					"session": session,
					"appname": name,
					"host":    host,
					"error":   r.err,
				}).Error("unable to get an application")
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return r.slave, nil

		case <-time.After(1 * time.Second):
			c.log.WithFields(logrus.Fields{
				"session": session,
				"appname": name,
				"host":    host,
				"error":   ErrResolvingTimeout,
			}).Error("service resolvation was timeouted")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *cocaineResolver) Close() {
	close(c.onClose)
}

func getRandomHost(input []string) string {
	max := len(input)
	return input[rand.Intn(max)]
}
