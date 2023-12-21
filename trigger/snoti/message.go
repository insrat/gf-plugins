package snoti

import (
	"encoding/json"
)

type GeneralResponse struct {
	Cmd       string `json:"cmd"`
	EventType string `json:"event_type"`
	ErrorCode int    `json:"error_code"`
	Message   string `json:"msg"`
}

type LoginRequest struct {
	Cmd           string             `json:"cmd"`
	PrefetchCount int                `json:"prefetch_count"`
	Data          []LoginRequestData `json:"data"`
}

type LoginRequestData struct {
	ProductKey string   `json:"product_key"`
	AuthID     string   `json:"auth_id"`
	AuthSecret string   `json:"auth_secret"`
	SubKey     string   `json:"subkey"`
	Events     []string `json:"events"`
}

type LoginResponse struct {
	Cmd  string `json:"cmd"`
	Data struct {
		Result  bool   `json:"result"`
		Message string `json:"msg"`
	} `json:"data"`
}

type PingRequest struct {
	Cmd string `json:"cmd"`
}

type AckRequest struct {
	Cmd        string `json:"cmd"`
	MsgId      string `json:"msg_id"`
	DeliveryId int64  `json:"delivery_id"`
}

type EventResponse struct {
	Cmd        string `json:"cmd"`
	MsgId      string `json:"msg_id"`
	DeliveryId int64  `json:"delivery_id"`
}

type OnlineAndOfflineEvent struct {
	Cmd        string  `json:"cmd"`
	MsgId      string  `json:"msg_id"`
	DeliveryId int64   `json:"delivery_id"`
	EventType  string  `json:"event_type"`
	ProductKey string  `json:"product_key"`
	DeviceId   string  `json:"did"`
	DeviceMac  string  `json:"mac"`
	CreatedAt  float64 `json:"created_at"`
}

type FaultAndAlertEvent struct {
	Cmd        string `json:"cmd"`
	MsgId      string `json:"msg_id"`
	DeliveryId string `json:"delivery_id"`
	EventType  string `json:"event_type"`
	ProductKey string `json:"product_key"`
	DeviceId   string `json:"did"`
	DeviceMac  string `json:"mac"`
	Data       struct {
		AttrName        string `json:"attr_name"`
		AttrDisplayName string `json:"attr_displayname"`
		Value           int    `json:"value"`
	} `json:"data"`
	CreatedAt float64 `json:"created_at"`
}

type KeyValueEvent struct {
	Cmd        string                 `json:"cmd"`
	MsgId      string                 `json:"msg_id"`
	DeliveryId string                 `json:"delivery_id"`
	EventType  string                 `json:"event_type"`
	ProductKey string                 `json:"product_key"`
	DeviceId   string                 `json:"did"`
	DeviceMac  string                 `json:"mac"`
	Data       map[string]interface{} `json:"data"`
	CreatedAt  float64                `json:"created_at"`
}

func Encode(v interface{}) []byte {
	buff, _ := json.Marshal(v)
	return append(buff, byte('\n'))
}

func Decode(b []byte) GeneralResponse {
	var v GeneralResponse
	_ = json.Unmarshal(b, &v)
	return v
}
