package snoti

import (
	"context"
	"time"

	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
	"github.com/project-flogo/core/trigger"
)

var triggerMd = trigger.NewMetadata(&Settings{}, &HandlerSettings{}, &Output{})

func init() {
	_ = trigger.Register(&Trigger{}, &Factory{})
}

// Factory is a trigger factory
type Factory struct {
}

// Metadata implements trigger.Factory.Metadata
func (*Factory) Metadata() *trigger.Metadata {
	return triggerMd
}

// New implements trigger.Factory.New
func (*Factory) New(config *trigger.Config) (trigger.Trigger, error) {
	s := &Settings{}
	err := metadata.MapToStruct(config.Settings, s, true)
	if err != nil {
		return nil, err
	}

	return &Trigger{settings: s}, nil
}

// Trigger is a kafka trigger
type Trigger struct {
	settings *Settings
	conn     Connection
	shutdown chan struct{}
	msgChs   map[string]chan []byte
	handlers []trigger.Handler
	logger   log.Logger
}

// Initialize initializes the trigger
func (t *Trigger) Initialize(ctx trigger.InitContext) (err error) {
	t.handlers = ctx.GetHandlers()
	t.logger = ctx.Logger()
	t.conn = NewConnection(ctx.Logger(), t.settings)
	return
}

// Start starts the kafka trigger
func (t *Trigger) Start() error {
	go t.conn.Connect()

	t.shutdown = make(chan struct{})
	t.msgChs = make(map[string]chan []byte)
	for _, handler := range t.handlers {
		t.msgChs[handler.Name()] = make(chan []byte, 128)
		go t.runHandler(handler)
	}
	go t.distributeMessages()
	// Waiting for ready.
	time.Sleep(1 * time.Second)

	return nil
}

// Stop implements ext.Trigger.Stop
func (t *Trigger) Stop() error {
	close(t.shutdown)
	t.conn.Close()
	return nil
}

func (t *Trigger) distributeMessages() {
	for {
		select {
		case <-t.shutdown:
			return
		default:
		}
		if buff := t.conn.Read(); buff != nil {
			for _, msgCh := range t.msgChs {
				select {
				case msgCh <- buff:
				default:
					<-msgCh
					msgCh <- buff
				}
			}
		}
	}
}

func (t *Trigger) runHandler(handler trigger.Handler) {
	msgCh := t.msgChs[handler.Name()]
	for {
		select {
		case <-t.shutdown:
			return
		case buff := <-msgCh:
			var output Output
			output.Message = string(buff)
			if _, err := handler.Handle(context.Background(), output); err != nil {
				t.logger.Errorf("run action for handler [%s] failed for reason [%s] message lost", handler.Name(), err)
			}
		}
	}
}
