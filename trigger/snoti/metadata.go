package snoti

import (
	"encoding/json"
)

var defaultEventTypes = []string{
	"device.online",
	"device.offline",
	"device.status.kv",
	"device.attrs_fault",
	"device.attr_alert",
}

type Settings struct {
	BrokerUrl  string `md:"brokerUrl,required"`
	AuthID     string `md:"authID,required"`
	AuthSecret string `md:"authSecret,required"`
	ProductKey string `md:"productKey,required"`
	SubKey     string `md:"subKey,required"`
}

type HandlerSettings struct {
}

type Output struct {
	Message string `md:"message"`
}

func (o *Output) ToMap() map[string]interface{} {
	values := make(map[string]interface{})
	_ = json.Unmarshal([]byte(o.Message), &values)
	return values
}

func (o *Output) FromMap(values map[string]interface{}) error {
	message, err := json.Marshal(values)
	if err != nil {
		return err
	}
	o.Message = string(message)
	return nil
}
