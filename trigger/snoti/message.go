package snoti

import (
	"encoding/json"
)

type GeneralResponse struct {
	Cmd       string `json:"cmd"`
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

func Encode(v interface{}) []byte {
	buff, _ := json.Marshal(v)
	return append(buff, byte('\n'))
}

func Decode(b []byte) GeneralResponse {
	var v GeneralResponse
	_ = json.Unmarshal(b, &v)
	return v
}

func BuildAck(b []byte) []byte {
	var ack AckRequest
	_ = json.Unmarshal(b, &ack)
	ack.Cmd = "event_ack"
	return Encode(ack)
}
