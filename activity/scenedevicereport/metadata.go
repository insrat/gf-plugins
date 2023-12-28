package scenedevicereport

import (
	"fmt"
	"strings"

	"github.com/project-flogo/core/data/coerce"
)

const (
	cacheUpdateTimeKey = "_update_time"
)

type Settings struct {
	RedisUrl string `md:"redisUrl,required"`
	MySQLUrl string `md:"mysqlUrl,required"`
}

type Input struct {
	ProductKey string                 `md:"productKey"`
	DeviceID   string                 `md:"deviceId"`
	DeviceMac  string                 `md:"deviceMac"`
	EventType  string                 `md:"eventType"`
	EventData  map[string]interface{} `md:"eventData"`
	EventTime  float64                `md:"eventTime"`
}

func (i *Input) CacheKey() string {
	return fmt.Sprintf("%s:%s", i.ProductKey, i.DeviceMac)
}

func (i *Input) CacheValue() map[string]interface{} {
	i.EventData[cacheUpdateTimeKey] = i.EventTime
	return i.EventData
}

// FromMap converts the values from a map into the struct Input
func (i *Input) FromMap(values map[string]interface{}) (err error) {
	i.ProductKey, err = coerce.ToString(values["productKey"])
	if err != nil {
		return
	}
	i.DeviceID, err = coerce.ToString(values["deviceId"])
	if err != nil {
		return
	}
	i.DeviceMac, err = coerce.ToString(values["deviceMac"])
	if err != nil {
		return
	}
	i.EventType, err = coerce.ToString(values["eventType"])
	if err != nil {
		return
	}
	i.EventData, err = coerce.ToObject(values["eventData"])
	if err != nil {
		return
	}
	i.EventTime, err = coerce.ToFloat64(values["eventTime"])
	return
}

// ToMap converts the struct Input into a map
func (i *Input) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"productKey": i.ProductKey,
		"deviceId":   i.DeviceID,
		"deviceMac":  i.DeviceMac,
		"eventType":  i.EventType,
		"eventData":  i.EventData,
		"eventTime":  i.EventTime,
	}
}

type Output struct {
	SceneIDs []interface{} `md:"sceneIDs"`
}

// FromMap converts the values from a map into the struct Output
func (o *Output) FromMap(values map[string]interface{}) (err error) {
	o.SceneIDs, err = coerce.ToArray(values["sceneIDs"])
	return
}

// ToMap converts the struct Output into a map
func (o *Output) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"sceneIDs": o.SceneIDs,
	}
}

func intSliceToString(in []int64) string {
	var out []string
	for _, v := range in {
		out = append(out, fmt.Sprint(v))
	}
	return strings.Join(out, ",")
}
