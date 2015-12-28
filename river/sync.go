package river

import (
	"github.com/puper/go-jsonrpc/jsonrpc"
	"github.com/puper/go-mysql/canal"
	"time"
	"log"
)

const (
	fieldTypeList = "list"
)

type rowsEventHandler struct {
	r *River
}

func NewRequest(e *canal.RowsEvent) *map[string]interface{} {
	r := make(map[string]interface{})
	r["table"] = e.Table.Name
	r["action"] = e.Action
	columns := make([]string, len(e.Table.Columns))
	for i, column := range e.Table.Columns {
		columns[i] = column.Name
	}
	r["columns"] = columns
	r["rows"] = e.Rows
	return &r
}

type Row struct {
	Old []interface{}
	Changed map[int]interface{}
}

func (h *rowsEventHandler) Do(e *canal.RowsEvent) error {
	rules, ok := h.r.rules[ruleKey(e.Table.Schema, e.Table.Name)]
	if !ok {
		return nil
	}
	request := NewRequest(e)
	for _, rule := range rules {
		h.r.requestWg.Add(1)
		go h.r.makeRequest(rule, request)
	}
	h.r.requestWg.Wait()
	return nil
}

func (h *rowsEventHandler) String() string {
	return "RiverRowsEventHandler"
}

func (r *River) makeRequest(rule *Rule, request *map[string]interface{}) error {
	defer r.requestWg.Done()
	client := jsonrpc.NewClient(rule.Url)
	i := 0
	for {
		resp, err := client.Call(rule.Method, request)
		if err == nil && resp.Error == nil {
			return nil
		}
		log.Println(err)
		time.Sleep(time.Second * time.Duration(i))
		if i < 3 {
			i++
		}
	}
}
