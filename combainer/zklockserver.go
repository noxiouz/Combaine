package combainer

import (
	"fmt"
	"os"
	"strings"
	"time"

	"launchpad.net/gozk/zookeeper"
)

type Lockserver interface {
	Lock(string) <-chan error
	Unlock(string) error
	Close() error
	OnDisconnection() <-chan struct{}
}

type LockserverConfig struct {
	Id      string   "app_id"
	Hosts   []string "host"
	Name    string   "name"
	timeout uint     "timeout"
}

type zkLockServer struct {
	Zk               *zookeeper.Conn
	session          <-chan zookeeper.Event
	config           LockserverConfig
	stop             chan struct{}
	on_disconnection chan struct{}
}

func NewLockServer(config LockserverConfig) (Lockserver, error) {
	connection_string := strings.Join(config.Hosts, ",")
	LogInfo("Connecting to %s", connection_string)
	zk, session, err := zookeeper.Dial(connection_string, 5e9)
	if err != nil {
		LogErr("Zookeeper connection error %s", err)
		return nil, err
	}

	select {
	case event := <-session:
		LogInfo("On Zookeeper connection event %s", event)
	case <-time.After(time.Second * 5):
		return nil, fmt.Errorf("Connection timeout")
	}
	return &zkLockServer{
		Zk:               zk,
		session:          session,
		config:           config,
		stop:             make(chan struct{}),
		on_disconnection: make(chan struct{}),
	}, nil
}

func (ls *zkLockServer) Lock(lockname string) <-chan error {
	full_lockname := fmt.Sprintf("/%s/%s", ls.config.Id, lockname)
	LogInfo("Try creating lock %s", full_lockname)
	// Add hostname as key payload
	DUMMY, _ := os.Hostname()

	path, err := ls.Zk.Create(full_lockname, DUMMY, zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		LogInfo("Unable to create lock %s: %s", full_lockname, err)
		return nil
	}

	return ls.poller(path)
}

func (ls *zkLockServer) Unlock(lockname string) error {
	return ls.Zk.Delete(lockname, -1)
}

func (ls *zkLockServer) Close() error {
	close(ls.stop)
	ls.Zk.Close()
	return nil
}

func (ls *zkLockServer) OnDisconnection() <-chan struct{} {
	return ls.on_disconnection
}

func (ls *zkLockServer) poller(path string) <-chan error {
	notify := make(chan error, 1)
	go func() {
		getWatcher := func(path string) (watcher <-chan zookeeper.Event, err error) {
			_, _, watcher, err = ls.Zk.GetW(path)
			return
		}

		watcher, err := getWatcher(path)
		if err != nil {
			LogErr("Unable to attach Zookeeper watcher to %s: %s", path, err)
			// it's better to send this to notify client
			// about bad situation
			notify <- err
			return
		}

		for {
			select {
			case event := <-ls.session:
				LogInfo("Receive poller event %s", event)
				if !event.Ok() {
					close(ls.on_disconnection)
				}
			case <-ls.stop:
				LogInfo("Stop the poller")
				return
			case event := <-watcher:
				if !event.Ok() {
					LogErr("Receive not OK event from Zookeeper %s", event.String())
					notify <- fmt.Errorf(event.String())
					return
				} else if event.Type == zookeeper.EVENT_DELETED {
					err := fmt.Errorf("Lock `%s` has been deleted", path)
					LogErr(err.Error())
					notify <- err
				} else {
					watcher, err = getWatcher(path)
					if err != nil {
						LogErr("Unable to attach Zookeeper watcher to %s: %s", path, err)
						// it's better to send this to notify client
						// about bad situation
						notify <- err
						return
					}
				}
			}
		}
	}()

	return notify
}
