package snoti

import (
	"github.com/project-flogo/core/data/coerce"
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
	return map[string]interface{}{"message": o.Message}
}

func (o *Output) FromMap(values map[string]interface{}) (err error) {
	o.Message, err = coerce.ToString(values["message"])
	return
}
