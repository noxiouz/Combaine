package slave

import (
	"golang.org/x/net/context"
	"sync"
	"sync/atomic"

	"github.com/cocaine/cocaine-framework-go/cocaine"
)

type cacheMap map[string]Slave

type localCocaineResolver struct {
	cache atomic.Value
	mu    sync.Mutex
}

func (l *localCocaineResolver) Resolve(_ context.Context, name string) (Slave, error) {
	cachemap := l.cache.Load().(cacheMap)
	if slave, ok := cachemap[name]; ok {
		return slave, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	cachemap = l.cache.Load().(cacheMap)
	if slave, ok := cachemap[name]; ok {
		return slave, nil
	}

	app, err := cocaine.NewService(name)
	if err != nil {
		return nil, err
	}

	slv := NewSlave(app)
	cachemap[name] = slv
	l.cache.Store(cachemap)
	return slv, nil
}

func (l *localCocaineResolver) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	cachemap := l.cache.Load().(cacheMap)
	for _, slave := range cachemap {
		slave.Close()
	}
}

func NewLocalResolver() Resolver {
	var cache atomic.Value
	cache.Store(make(cacheMap))

	return &localCocaineResolver{
		cache: cache,
	}
}
