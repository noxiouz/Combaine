package main

import (
	"log"
	"runtime"

	"github.com/cocaine/cocaine-framework-go/cocaine"

	"github.com/noxiouz/Combaine/common"
	"github.com/noxiouz/Combaine/common/tasks"
	"github.com/noxiouz/Combaine/parsing"

	_ "github.com/noxiouz/Combaine/fetchers/httpfetcher"
	_ "github.com/noxiouz/Combaine/fetchers/rawsocket"
	_ "github.com/noxiouz/Combaine/fetchers/timetail"
)

var logger *cocaine.Logger

func handleTask(request *cocaine.Request, response *cocaine.Response) {
	defer response.Close()
	raw := <-request.Read()
	var task tasks.ParsingTask
	err := common.Unpack(raw, &task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
		return
	}
	result, err := parsing.Parsing(task)
	if err != nil {
		response.ErrorMsg(-100, err.Error())
	} else {
		res, _ := common.Pack(result)
		response.Write(res)
	}
}

func main() {
	runtime.GOMAXPROCS(10)
	var err error
	logger, err = cocaine.NewLogger()
	binds := map[string]cocaine.EventHandler{
		"handleTask": handleTask,
	}
	Worker, err := cocaine.NewWorker()
	if err != nil {
		log.Fatal(err)
	}
	Worker.Loop(binds)
}
