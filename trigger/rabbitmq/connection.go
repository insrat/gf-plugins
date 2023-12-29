package rabbitmq

import (
	"strings"
	"time"

	"github.com/project-flogo/core/support/log"
	"github.com/streadway/amqp"
)

type Connection struct {
	settings *Settings
	conn     *amqp.Connection
	channel  *amqp.Channel
	logger   log.Logger
}

func (c *Connection) Connection() (<-chan amqp.Delivery, error) {
	return c.channel.Consume(
		c.settings.QueueName,
		"flogo-scene",    // 消费者标识
		c.settings.NoAck, // 显式确认
		false,            // 不独占
		false,            // 不等待服务器响应
		false,            // 不阻塞
		nil,
	)
}

func (c *Connection) Stop() {
	if c.channel != nil {
		_ = c.channel.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *Connection) autoReconnect() {
	closeCh := make(chan *amqp.Error)
	c.conn.NotifyClose(closeCh)

	var err error
	for {
		closeErr := <-closeCh
		if closeErr != nil {
			c.logger.Errorf("connection closed, reconnecting in 5 seconds: %v", closeErr)
			time.Sleep(5 * time.Second)

			c.conn, err = c.connect()
			if err != nil {
				c.logger.Errorf("failed to reconnecting to RabbitMQ: %v", err)
				continue
			}
			c.logger.Infof("reconnected to RabbitMQ")
			closeCh = make(chan *amqp.Error)
			c.conn.NotifyClose(closeCh)
		}
	}
}

func (c *Connection) connect() (*amqp.Connection, error) {
	conn, err := amqp.Dial(c.settings.BrokerUrl)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func getRabbitMQConnection(settings *Settings) (*Connection, error) {
	newConn := new(Connection)
	newConn.settings = settings

	var err error
	// Create connect and channel with broker URL.
	if newConn.conn, err = newConn.connect(); err != nil {
		return nil, err
	}
	if newConn.channel, err = newConn.conn.Channel(); err != nil {
		return nil, err
	}

	// Set channel prefetch count.
	if err = newConn.channel.Qos(int(settings.PrefetchCount), 0, false); err != nil {
		return nil, err
	}

	// Set up consumption queue.
	if _, err = newConn.channel.QueueDeclare(
		settings.QueueName, // 队列名
		true,               // 持久性
		false,              // 删除时没有消费者时自动删除队列
		false,              // 独占队列
		false,              // 不等待服务器响应
		nil,
	); err != nil {
		return nil, err
	}

	// Bind consumption routing to queue.
	routingKeys := strings.Split(settings.RoutingKeys, ",")
	for _, routingKey := range routingKeys {
		if err = newConn.channel.QueueBind(
			settings.QueueName,    // 队列名
			routingKey,            // 路由键值
			settings.ExchangeName, // 交换机名
			false,
			nil,
		); err != nil {
			return nil, err
		}
	}

	go newConn.autoReconnect()

	return newConn, nil
}
