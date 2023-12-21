package snoti

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
	Cmd        string                 `md:"cmd"`
	MsgId      string                 `md:"msg_id"`
	DeliveryId int64                  `md:"delivery_id"`
	EventType  string                 `md:"event_type"`
	ProductKey string                 `md:"product_key"`
	DeviceId   string                 `md:"did"`
	DeviceMac  string                 `md:"mac"`
	Data       map[string]interface{} `md:"data"`
	CreatedAt  float64                `md:"created_at"`
}
