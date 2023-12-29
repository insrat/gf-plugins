package rabbitmq

import (
	"github.com/project-flogo/core/data/coerce"
)

type Settings struct {
	BrokerUrl     string `md:"brokerUrl,required"`
	ExchangeName  string `md:"exchangeName,required"`
	QueueName     string `md:"queueName,required"`
	RoutingKeys   string `md:"routingKeys,required"`
	PrefetchCount int64  `md:"prefetchCount,required"`
	NoAck         bool   `md:"noAck,required"`
}

type Output struct {
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
