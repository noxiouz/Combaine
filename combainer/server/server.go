package server

import (
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/noxiouz/Combaine/vendor/launchpad.net/gozk/zookeeper"

	"github.com/noxiouz/Combaine/combainer"
	"github.com/noxiouz/Combaine/combainer/discovery"
	"github.com/noxiouz/Combaine/combainer/lockserver"
	"github.com/noxiouz/Combaine/combainer/slave"
	"github.com/noxiouz/Combaine/common/cache"
	"github.com/noxiouz/Combaine/common/configs"
)

var (
	SHOULD_WAIT   bool = true
	GEN_UNIQUE_ID      = ""
)

func Trap() {
	if r := recover(); r != nil {
		logrus.Printf("Recovered: %s", r)
	}
}

type CombaineServer struct {
	Configuration   CombaineServerConfig
	CombainerConfig configs.CombainerConfig

	configs.Repository
	cache.Cache
	*combainer.Context
	log *logrus.Entry
}

type CombaineServerConfig struct {
	// Configuration
	// path to directory with combaine.yaml
	ConfigsPath string
	// period of the locks rechecking
	Period time.Duration
	// Addrto listen for incoming http REST API requests
	RestEndpoint string
	//
	Active bool
}

func NewCombainer(config CombaineServerConfig) (*CombaineServer, error) {
	log := logrus.WithField("source", "server")
	repository, err := configs.NewFilesystemRepository(config.ConfigsPath)
	if err != nil {
		log.Fatalf("unable to initialize filesystemRepository: %s", err)
	}

	combainerConfig := repository.GetCombainerConfig()
	if err := configs.VerifyCombainerConfig(&combainerConfig); err != nil {
		log.Fatalf("malformed combainer config: %s", err)
	}

	cacheCfg := &combainerConfig.MainSection.Cache
	cacheType, err := cacheCfg.Type()
	if err != nil {
		log.Fatalf("unable to get type of cache: %s", err)
	}

	cacher, err := cache.NewCache(cacheType, cacheCfg)
	if err != nil {
		log.Fatalf("unable to initialize cache: %s", err)
	}

	// Get Combaine hosts
	cloud_group := combainerConfig.MainSection.CloudGroup
	context := &combainer.Context{
		Cache: cacher,
		// ToDo: should setup in New*
		Discovery: nil,
		Logger:    logrus.StandardLogger(),
	}

	s, err := combainer.LoadHostFetcher(context, combainerConfig.CloudSection.HostFetcher)
	if err != nil {
		return nil, err
	}

	context.Discovery = discovery.NewHTTPDiscovery(func() ([]string, error) {
		h, err := s.Fetch(cloud_group)
		if err != nil {
			return nil, err
		}
		return h.AllHosts(), nil
	})

	context.Resolver = slave.NewCocaineResolver(context.Discovery)

	server := &CombaineServer{
		Configuration:   config,
		CombainerConfig: combainerConfig,
		Repository:      repository,
		Cache:           cacher,
		Context:         context,
		log:             log,
	}

	return server, nil
}

func (c *CombaineServer) GetContext() *combainer.Context {
	return c.Context
}

func (c *CombaineServer) GetRepository() configs.Repository {
	return c.Repository
}

func (c *CombaineServer) Serve() error {
	c.log.Info("starting REST API")
	router := combainer.GetRouter(c)
	go func() {
		err := http.ListenAndServe(c.Configuration.RestEndpoint, router)
		if err != nil {
			c.log.Fatal("ListenAndServe: ", err)
		}
	}()

	if c.Configuration.Active {
		c.log.Info("start task distribution")
		go c.distributeTasks()
	}

	sigWatcher := make(chan os.Signal, 1)
	signal.Notify(sigWatcher, os.Interrupt, os.Kill)
	s := <-sigWatcher
	c.log.Info("Got signal:", s)
	return nil
}

func (c *CombaineServer) distributeTasks() {
LOCKSERVER_LOOP:
	for {
		DLS, err := lockserver.NewLockServer(c.CombainerConfig.LockServerSection)
		if err != nil {
			c.log.WithFields(logrus.Fields{
				"error": err,
			}).Error("unable to create Zookeeper lockserver")
			time.Sleep(c.Configuration.Period)
			continue LOCKSERVER_LOOP
		}

		var next <-chan time.Time
		next = time.After(time.Millisecond * 10)

	DISPATCH_LOOP:
		for {
			select {
			// Spawn one more client
			case <-next:
				next = time.After(c.Configuration.Period)

				configs, err := c.Repository.ListParsingConfigs()
				if err != nil {
					c.log.WithFields(logrus.Fields{
						"error": err,
					}).Error("unable to list parsing configs")
					continue DISPATCH_LOOP
				}

				go func(configs []string) {

					for _, cfg := range configs {
						lockerr := DLS.Lock(cfg)
						if lockerr != nil {
							continue
						}

						lockname := cfg

						// Inline function to use defers
						func(lockname string) {
							defer DLS.Unlock(lockname)
							defer Trap()

							if !c.Repository.ParsingConfigIsExists(cfg) {
								c.log.WithField("error", "config doesn't exist").Error(cfg)
								return
							}

							c.log.Info("creating new client", lockname)
							cl, err := combainer.NewClient(c.Context, c.Repository)
							// think about unique ID for metrics name
							combainer.GlobalMetrics.RegisterRegistry(cl.Registry, lockname)
							defer combainer.GlobalMetrics.UnregisterRegistry(lockname)

							if err != nil {
								c.log.WithFields(logrus.Fields{
									"error":    err,
									"lockname": lockname,
								}).Error("can't create client")
								return
							}

							var watcher <-chan zookeeper.Event
							watcher, err = DLS.Watch(lockname)
							if err != nil {
								c.log.WithFields(logrus.Fields{
									"error":    err,
									"lockname": lockname,
								}).Error("can't watch")
								return
							}

							for {
								if err := cl.Dispatch(lockname, GEN_UNIQUE_ID, SHOULD_WAIT); err != nil {
									c.log.WithFields(logrus.Fields{
										"error":    err,
										"lockname": lockname,
									}).Error("Dispatch error")
									return
								}
								select {
								case event := <-watcher:
									if !event.Ok() || event.Type == zookeeper.EVENT_DELETED {
										c.log.Error("lock has been lost: %s", event)
										return
									}
									watcher, err = DLS.Watch(lockname)
									if err != nil {
										c.log.WithFields(logrus.Fields{
											"error":    err,
											"lockname": lockname,
										}).Error("can't watch")
										return
									}
								default:
								}
							}
						}(lockname)
					}
				}(configs)
			case event := <-DLS.Session:
				if !event.Ok() {
					c.log.Errorf("not OK event from Zookeeper: %s", event)
					DLS.Close()
					break DISPATCH_LOOP
				}
			}

		}
	}

}
