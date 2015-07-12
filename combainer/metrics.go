package combainer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/noxiouz/Combaine/vendor/github.com/rcrowley/go-metrics"
	"io"
	"net/http"
	"runtime"
	"sync"
	"time"
	// "syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/kr/pretty"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/configs"
)

func init() {
	registry := metrics.NewRegistry()
	goroutines := metrics.NewRegisteredHistogram(
		"goroutines",
		registry,
		metrics.NewUniformSample(1024))

	GlobalMetrics.RegisterRegistry(registry, "combainer.system")

	go func() {
		t := time.NewTicker(time.Second * 5)
		for _ = range t.C {
			goroutines.Update(int64(runtime.NumGoroutine()))
		}
	}()
}

var GlobalMetrics = Metrics{
	registries: make(map[string]metrics.Registry),
}

type Metrics struct {
	sync.RWMutex
	registries map[string]metrics.Registry
}

func (m *Metrics) RegisterRegistry(r metrics.Registry, name string) {
	m.RWMutex.Lock()
	m.registries[name] = r
	m.RWMutex.Unlock()
}

func (m *Metrics) UnregisterRegistry(name string) {
	m.RWMutex.Lock()
	delete(m.registries, name)
	m.RWMutex.Unlock()
}

func (m *Metrics) GetRegistries() map[string]metrics.Registry {
	m.RLock()
	defer m.RUnlock()
	cpy := make(map[string]metrics.Registry, len(m.registries))
	for k, v := range m.registries {
		cpy[k] = v
	}
	return cpy
}

func (m *Metrics) GetRegistry(config string) metrics.Registry {
	m.RLock()
	defer m.RUnlock()
	return m.registries[config]
}

func JsonToWriter(w io.Writer, v interface{}) error {
	var out bytes.Buffer
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	json.Indent(&out, b, "", "\t")
	_, err = out.WriteTo(w)
	return err
}

func Dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	b, _ := json.Marshal(GlobalMetrics.GetRegistries())
	var out bytes.Buffer

	json.Indent(&out, b, "", "\t")
	out.WriteTo(w)
}

func ParsingConfigs(s ServerContext, w http.ResponseWriter, r *http.Request) {
	list, _ := s.GetRepository().ListParsingConfigs()
	json.NewEncoder(w).Encode(&list)
}

func ReadParsingConfig(s ServerContext, w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	repo := s.GetRepository()
	combainerCfg := repo.GetCombainerConfig()
	var parsingCfg configs.ParsingConfig
	cfg, err := repo.GetParsingConfig(name)
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	err = cfg.Decode(&parsingCfg)
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	parsingCfg.UpdateByCombainerConfig(&combainerCfg)
	aggregationConfigs, err := GetAggregationConfigs(repo, &parsingCfg)
	if err != nil {
		log.Errorf("Unable to read aggregation configs: %s", err)
		return
	}

	data, err := common.Encode(&parsingCfg)
	if err != nil {
		fmt.Fprintf(w, "%s", err)
		return
	}

	fmt.Fprintf(w, "============ %s ============\n", name)
	fmt.Fprintf(w, "%s", data)
	for aggname, v := range *aggregationConfigs {
		fmt.Fprintf(w, "============ %s ============\n", aggname)
		d, err := common.Encode(&v)
		if err != nil {
			fmt.Fprintf(w, "%s", err)
			return
		}
		fmt.Fprintf(w, "%s\n", d)
	}
}

func Tasks(s ServerContext, w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	cl, err := NewClient(s.GetContext(), s.GetRepository())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sp, err := cl.UpdateSessionParams(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for n, task := range sp.PTasks {
		fmt.Fprintf(w, "============ (%d/%d) ============\n", n+1, len(sp.PTasks))
		fmt.Fprintf(w, "%# v\n", pretty.Formatter(task))
	}
}

func Launch(s ServerContext, w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	name := mux.Vars(r)["name"]

	logger := log.New()
	logger.Level = log.DebugLevel
	logger.Formatter = s.GetContext().Logger.Formatter
	logger.Out = w

	ctx := &Context{
		Logger:    logger,
		Cache:     s.GetContext().Cache,
		Discovery: s.GetContext().Discovery,
		Resolver:  s.GetContext().Resolver,
	}

	cl, err := NewClient(ctx, s.GetRepository())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ID := GenerateSessionId()
	err = cl.Dispatch(name, ID, false)
	fmt.Fprintf(w, "%s\n", ID)
	w.(http.Flusher).Flush()
	defer JsonToWriter(w, cl.Stats.Registry)
	if err != nil {
		fmt.Fprintf(w, "FAILED: %v\n", err)
		return
	}
	fmt.Fprint(w, "DONE")
}

type ServerContext interface {
	GetContext() *Context
	GetRepository() configs.Repository
}

func attachServer(s ServerContext,
	wrapped func(s ServerContext, w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		wrapped(s, w, r)
	}
}

func GetRouter(context ServerContext) http.Handler {
	root := mux.NewRouter()
	root.StrictSlash(true)

	parsingRouter := root.PathPrefix("/parsing/").Subrouter()
	parsingRouter.StrictSlash(true)
	parsingRouter.HandleFunc("/", attachServer(context, ParsingConfigs)).Methods("GET")
	parsingRouter.HandleFunc("/{name}", attachServer(context, ReadParsingConfig)).Methods("GET")

	root.HandleFunc("/tasks/{name}", attachServer(context, Tasks)).Methods("GET")
	root.HandleFunc("/launch/{name}", attachServer(context, Launch)).Methods("GET")
	root.HandleFunc("/", Dashboard).Methods("GET")

	return root
}
