package main

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"github.com/juju/errors"
	"github.com/puper/go-mysql-listener/river"
)

var configFile = flag.String("config", "./etc/river.json", "go-mysql-listenser config file")

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	cfg, err := river.NewConfigWithFile(*configFile)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}
	r, err := river.NewRiver(cfg)
	if err != nil {
		println(errors.ErrorStack(err))
		return
	}

	r.Run()

	<-sc
	r.Close()
}
