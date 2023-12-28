package snoti

import (
	"context"
	"encoding/json"
	"runtime"
	"sync"
	"time"

	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
	"github.com/project-flogo/core/trigger"
)

var triggerMd = trigger.NewMetadata(&Settings{}, &Output{})

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
	settings  *Settings
	conn      Connection
	shutdown  chan struct{}
	messageCh chan []byte
	handlers  []trigger.Handler
	logger    log.Logger
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
	t.shutdown = make(chan struct{})
	t.messageCh = make(chan []byte)

	go t.conn.Connect()
	go t.receiveMessage()
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go t.handleMessage()
	}
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

func (t *Trigger) receiveMessage() {
	for {
		select {
		case <-t.shutdown:
			return
		default:
		}
		if buff := t.conn.Read(); buff != nil {
			t.messageCh <- buff
		}
	}
}

func (t *Trigger) handleMessage() {
	for {
		select {
		case <-t.shutdown:
			return
		case buff := <-t.messageCh:
			data := &Output{}
			if err := json.Unmarshal(buff, data); err != nil {
				continue
			}

			var wg sync.WaitGroup
			wg.Add(len(t.handlers))
			for _, handler := range t.handlers {
				go func(handler trigger.Handler) {
					defer wg.Done()
					_, err := handler.Handle(context.Background(), data)
					if err != nil {
						t.logger.Errorf("run action for handler [%s] failed for reason [%s] message lost", handler.Name(), err)
					}
				}(handler)
			}
			wg.Wait()

			t.conn.Write(BuildAck(buff))
		}
	}
}
