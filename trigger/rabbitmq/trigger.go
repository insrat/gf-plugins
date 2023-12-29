package rabbitmq

import (
	"context"
	"encoding/json"
	"runtime"

	"github.com/project-flogo/core/data/metadata"
	"github.com/project-flogo/core/support/log"
	"github.com/project-flogo/core/trigger"
	"github.com/streadway/amqp"
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
	settings *Settings
	conn     *Connection
	shutdown chan struct{}
	handlers []trigger.Handler
	logger   log.Logger
}

// Initialize initializes the trigger
func (t *Trigger) Initialize(ctx trigger.InitContext) (err error) {
	t.handlers = ctx.GetHandlers()
	t.logger = ctx.Logger()
	t.conn, err = getRabbitMQConnection(t.logger, t.settings)
	return
}

// Start starts the kafka trigger
func (t *Trigger) Start() error {
	deliveries, err := t.conn.Connection()
	if err != nil {
		return err
	}
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go t.handleMessage(deliveries)
	}
	return nil
}

// Stop implements ext.Trigger.Stop
func (t *Trigger) Stop() error {
	t.conn.Stop()
	return nil
}

func (t *Trigger) handleMessage(deliveries <-chan amqp.Delivery) {
	var err error
	for d := range deliveries {
		data := &Output{}
		if err = json.Unmarshal(d.Body, data); err != nil {
			if !t.settings.NoAck {
				_ = d.Ack(false)
			}
			continue
		}

		for _, handler := range t.handlers {
			if _, err = handler.Handle(context.Background(), data); err != nil {
				t.logger.Errorf("run action for handler [%s] failed for reason [%s] message lost", handler.Name(), err)
				break
			}
		}

		if t.settings.NoAck {
			continue
		}
		if err == nil {
			_ = d.Ack(false)
		} else {
			_ = d.Nack(false, true)
		}
	}
}
