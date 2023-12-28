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

type Output struct {
	Cmd        string                 `json:"cmd"`
	MessageID  string                 `json:"msg_id"`
	DeliveryID int64                  `json:"delivery_id"`
	ProductKey string                 `md:"productKey" json:"product_key"`
	DeviceID   string                 `md:"deviceId" json:"did"`
	DeviceMac  string                 `md:"deviceMac" json:"mac"`
	EventType  string                 `md:"eventType" json:"event_type"`
	EventData  map[string]interface{} `md:"eventData" json:"data"`
	EventTime  float64                `md:"eventTime" json:"created_at"`
}

// FromMap converts the values from a map into the struct Output
func (o *Output) FromMap(values map[string]interface{}) (err error) {
	o.ProductKey, err = coerce.ToString(values["productKey"])
	if err != nil {
		return
	}
	o.DeviceID, err = coerce.ToString(values["deviceId"])
	if err != nil {
		return
	}
	o.DeviceMac, err = coerce.ToString(values["deviceMac"])
	if err != nil {
		return
	}
	o.EventType, err = coerce.ToString(values["eventType"])
	if err != nil {
		return
	}
	o.EventData, err = coerce.ToObject(values["eventData"])
	if err != nil {
		return
	}
	o.EventTime, err = coerce.ToFloat64(values["eventTime"])
	return
}

// ToMap converts the struct Output into a map
func (o *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"productKey": o.ProductKey,
		"deviceId":   o.DeviceID,
		"deviceMac":  o.DeviceMac,
		"eventType":  o.EventType,
		"eventData":  o.EventData,
		"eventTime":  o.EventTime,
	}
}
