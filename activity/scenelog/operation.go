package sceneaction

type Actions []Action

type Action struct {
	AutoSceneID   int64
	Sort          int64
	Delay         int64
	Type          bool
	ActionID      int64
	ManualSceneID int64
	Operation     Operation
}

func (a *Actions) Execute() error {
	return nil
}

type Operation struct {
	ActionID      int64
	ActionType    string
	ProductKey    string
	DeviceSno     string
	ControlAttrs  string
	NoticeType    string
	NoticeTargets string
}

func (c *Operation) Execute() error {
	if c.ActionType == "notice" {
		return c.noticeMessage()
	}
	return c.controlDevice()
}

func (c *Operation) controlDevice() error {
	return nil
}

func (c *Operation) noticeMessage() error {
	return nil
}
