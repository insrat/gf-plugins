package sceneaction

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type Actions []Action

func (a Actions) String() string {
	v, _ := json.Marshal(a)
	return string(v)
}

func (a Actions) Execute() error {
	for _, action := range a {
		if err := action.Execute(); err != nil {
			return err
		}
	}
	return nil
}

type Action struct {
	AutoSceneID   int64
	HomeID        int64
	Sort          int64
	Delay         int64
	Type          []uint8
	ActionID      sql.NullInt64
	ManualSceneID sql.NullInt64
	Operation     Operation
}

func (a Action) Execute() error {
	if a.Delay > 0 {
		time.Sleep(time.Duration(a.Delay) * time.Millisecond)
	}
	if a.Type[0] == 1 {
		return nil
	}
	return a.Operation.Execute(a.HomeID)
}

type Operation struct {
	ActionID      int64
	ActionType    string
	ProductKey    string
	DeviceSno     string
	DeviceAttrs   map[string]interface{}
	ControlAttrs  sql.NullString `json:"-"`
	NoticeType    string
	NoticeTargets sql.NullString
}

func (c *Operation) Execute(homeID int64) error {
	if c.ActionType == "notice" {
		return c.noticeMessage(homeID)
	}
	return c.controlDevice()
}

func (c *Operation) controlDevice() error {
	fmt.Println(c.ActionID, c.ProductKey, c.DeviceSno, c.DeviceAttrs)
	return nil
}

func (c *Operation) noticeMessage(homeID int64) error {
	fmt.Println(c.ActionID, c.NoticeType, homeID)
	return nil
}
