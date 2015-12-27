package river

import (
	"io/ioutil"
	"encoding/json"
	"github.com/juju/errors"
)

type Config  struct {
	DataDir string `json:"dataDir"`
	Mysql *MysqlConfig `json:"mysql"`
	Rules []*Rule `json:"rules"`
}

type MysqlConfig struct {
	ServerId uint32 `json:"serverId"`
	Addr string `json:"addr"`
	User string `json:"user"`
	Pass string `json:"pass"`
	DumpExec string `json:"mysqldump"`
	Dbs []string `json:"dbs"`
	IgnoreTables map[string][]string `json:"ignoreTables"`
}

type Rule struct {
	Db string `json:"db"`
	Table string `json:"table"`
	Url string `json:"url"`
	Method string `json:"method"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(data)
}

func NewConfig(data []byte) (*Config, error) {
	var c Config
	err := json.Unmarshal(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}
