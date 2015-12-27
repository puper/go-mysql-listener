package river

import (
	"fmt"
	"sync"

	"github.com/juju/errors"
	"github.com/puper/go-mysql/canal"
	"github.com/siddontang/go/log"
)

type River struct {
	c *Config

	canal *canal.Canal

	rules map[string][]*Rule

	quit chan struct{}
	wg   sync.WaitGroup
	requestWg sync.WaitGroup
}

func NewRiver(c *Config) (*River, error) {
	r := new(River)

	r.c = c

	r.quit = make(chan struct{})

	r.rules = make(map[string][]*Rule)

	var err error
	if err = r.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareRule(); err != nil {
		return nil, errors.Trace(err)
	}

	if err = r.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}

	// We must use binlog full row image
	if err = r.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}
	
	return r, nil
}

func (r *River) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.c.Mysql.Addr
	cfg.User = r.c.Mysql.User
	cfg.Password = r.c.Mysql.Pass
	cfg.Flavor = "mysql"
	cfg.DataDir = r.c.DataDir

	cfg.ServerID = r.c.Mysql.ServerId
	cfg.Dump.ExecutionPath = r.c.Mysql.DumpExec
	cfg.Dump.DiscardErr = false

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return err
}

func (r *River) prepareCanal() error {
	r.canal.AddDumpDatabases(r.c.Mysql.Dbs...)
	for db, tables := range r.c.Mysql.IgnoreTables {
		for _, table := range tables {
			r.canal.AddDumpIgnoreTables(db, table)
		}
	}
	r.canal.RegRowsEventHandler(&rowsEventHandler{r})
	return nil
}

func (r *River) prepareRule() error {
	for _, rule := range r.c.Rules {
		r.rules[ruleKey(rule.Db, rule.Table)] = append(r.rules[ruleKey(rule.Db, rule.Table)], rule)
	}
	return nil
}

func ruleKey(db string, table string) string {
	return fmt.Sprintf("%s:%s", db, table)
}

func (r *River) Run() error {
	if err := r.canal.Start(); err != nil {
		log.Errorf("start canal err %v", err)
		return err
	}

	return nil
}

func (r *River) Close() {
	log.Infof("closing river")
	close(r.quit)

	r.canal.Close()

	r.wg.Wait()
}
