package parsing

import (
	"fmt"
	"sync"

	"github.com/noxiouz/Combaine/common/tasks"
)

var fLock sync.Mutex
var fetchers = map[string]func(map[string]interface{}) (Fetcher, error){}

func Register(name string, f func(map[string]interface{}) (Fetcher, error)) {
	fLock.Lock()
	fetchers[name] = f
	fLock.Unlock()
}

type Fetcher interface {
	Fetch(task *tasks.FetcherTask) ([]byte, error)
}

func NewFetcher(name string, cfg map[string]interface{}) (f Fetcher, err error) {
	initializer, ok := fetchers[name]
	if !ok {
		err = fmt.Errorf("Fetcher %s isn't available", name)
		return
	}

	f, err = initializer(cfg)
	return
}

func unregister(name string) {
	fLock.Lock()
	delete(fetchers, name)
	fLock.Unlock()
}

type mockFetcher struct {
	data []byte
}

func (m *mockFetcher) Fetch(task *tasks.FetcherTask) ([]byte, error) {
	if m.data != nil {
		return m.data, nil
	}

	return nil, fmt.Errorf("No data fetcher error")
}

func newMockFetcher(cfg map[string]interface{}) (Fetcher, error) {
	m := &mockFetcher{
		data: nil,
	}

	if data, ok := cfg["data"].([]byte); ok {
		m.data = data
	}

	return m, nil
}
