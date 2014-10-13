package main

import (
	"flag"
	"io/ioutil"
	"log"
	"time"

	"net/http"
	_ "net/http/pprof"

	"launchpad.net/goyaml"

	"github.com/noxiouz/Combaine/combainer"
)

const (
	COMBAINER_PATH = "/etc/combaine/combaine.yaml"
	DEFAULT_PERIOD = 5
)

var (
	endpoint              string
	profiler              string
	logoutput             string
	loglevel              string
	combainer_config_path string
	period                uint
)

func init() {
	flag.StringVar(&endpoint, "observer", "0.0.0.0:9000", "HTTP observer port")
	flag.StringVar(&profiler, "profiler", "", "profiler host:port <0.0.0.0:10000>")
	flag.StringVar(&logoutput, "logoutput", "/dev/stderr", "path to logfile")
	flag.StringVar(&loglevel, "loglevel", "INFO", "loglevel (DEBUG|INFO|WARN|ERROR)")
	flag.StringVar(&combainer_config_path, "combainer_config_path", COMBAINER_PATH, "path to combainer.yaml")
	flag.UintVar(&period, "period", 5, "period of retrying new lock (sec)")
}

func Work(cfg combainer.CombainerConfig, ls combainer.Lockserver) {
	cl, err := combainer.NewClient(cfg, ls)
	if err != nil {
		log.Panicf("Unable to initialize client %s", err)
	}
	cl.Dispatch()
}

func main() {
	flag.Parse()

	combainer.InitializeCacher()
	combainer.InitializeLogger(loglevel, logoutput)

	if profiler != "" {
		log.Println("Profiler enabled")
		go func() {
			if err := http.ListenAndServe(profiler, nil); err != nil {
				log.Fatal(err)
			}
			log.Println("Launch profiler successfully on ", profiler)
		}()
	}

	go combainer.StartObserver(endpoint)

	data, err := ioutil.ReadFile(combainer_config_path)
	if err != nil {
		log.Fatalf("Unable to read combainer config %s", err)
	}

	// Parse combaine.yaml
	var combainer_config combainer.CombainerConfig
	err = goyaml.Unmarshal(data, &combainer_config)
	if err != nil {
		log.Fatalf("Unable to decode combainer config %s", err)
	}

	// Initialize distributed lock server
	lockserver, err := combainer.NewLockServer(combainer_config.Combainer.LockServerCfg)
	if err != nil {
		log.Fatalf("Unable to initialize lockserver: %s", err)
	}
	defer lockserver.Close()

	// Subscribe to lockserver state
	lockserver_is_disconnected := lockserver.OnDisconnection()

	for {
		//log.Println("Try to start client")
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Println("Recovered: ", r)
				}
			}()

			Work(combainer_config, lockserver)
		}()
		select {
		case <-lockserver_is_disconnected:
			log.Fatalf("lockserver has been disconnected")
		case <-time.After(time.Second * time.Duration(period)):
		}
	}
}
