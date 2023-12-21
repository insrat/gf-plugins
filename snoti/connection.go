package snoti

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/project-flogo/core/support/log"
)

type Connection interface {
	Connect()
	Close()
	Read() []byte
	Write([]byte)
}

func NewConnection(logger log.Logger, settings *Settings) Connection {
	return &client{
		settings:  settings,
		sendCh:    make(chan []byte, 128),
		receiveCh: make(chan []byte, 128),
		logger:    logger,
	}
}

var _ Connection = new(client)

type client struct {
	settings  *Settings
	tlsConn   *tls.Conn
	heartbeat time.Time
	shutdown  chan struct{}
	sendCh    chan []byte
	receiveCh chan []byte
	logger    log.Logger
}

func (c *client) Connect() {
	c.shutdown = make(chan struct{})
	for {
		s, err := net.Dial("tcp", c.settings.BrokerUrl)
		if err != nil {
			c.logger.Errorf("failed to connect broker %s: %v", c.settings.BrokerUrl, err)
			c.logger.Infof("reconnect broker %s after 15 seconds", c.settings.BrokerUrl)
			time.Sleep(15 * time.Second)
			continue
		}
		c.logger.Infof("connect broker %s successfully", c.settings.BrokerUrl)

		c.tlsConn = tls.Client(s, &tls.Config{InsecureSkipVerify: true})
		if err = c.tlsConn.Handshake(); err != nil {
			c.logger.Errorf("failed to handshake broker %s: %v", c.settings.BrokerUrl, err)
			c.logger.Infof("reconnect broker %s after 15 seconds", c.settings.BrokerUrl)
			time.Sleep(15 * time.Second)
			continue
		}
		c.logger.Infof("handshake broker %s successfully", c.settings.BrokerUrl)

		if err = c.login(); err != nil {
			c.logger.Errorf("failed to login broker %s: %v", c.settings.BrokerUrl, err)
			c.logger.Infof("reconnect broker %s after 15 seconds", c.settings.BrokerUrl)
			time.Sleep(15 * time.Second)
			continue
		}
		c.logger.Infof("login broker %s successfully", c.settings.BrokerUrl)

		ctx, cancel := context.WithCancel(context.Background())
		go c.read(ctx)
		go c.write(ctx)
		if isShutdown := c.ping(ctx, cancel); isShutdown {
			return
		}
		time.Sleep(1 * time.Second)

		c.logger.Warnf("the connection has been lost")
		_ = c.tlsConn.Close()
	}
}

func (c *client) Close() {
	close(c.shutdown)
	if err := c.tlsConn.Close(); err != nil {
		c.logger.Errorf("failed to close connection %s: %v", c.settings.BrokerUrl, err)
		return
	}
	c.logger.Infof("connection %s closed successfully", c.settings.BrokerUrl)
}

func (c *client) Read() []byte {
	select {
	case <-time.After(1 * time.Second):
		return nil
	case buff := <-c.receiveCh:
		return buff
	}
}

func (c *client) read(ctx context.Context) {
	buff := make([]byte, 1024)
	for {
		select {
		case <-ctx.Done():
			c.logger.Warnf("the connection has been closed, exiting the reading thread")
			return
		default:
		}
		if err := c.tlsConn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
			c.logger.Errorf("failed to set read deadline: %v", err)
			continue
		}
		n, err := c.tlsConn.Read(buff)
		if err != nil || n == 0 {
			continue
		}
		c.logger.Infof("read message successfully, the content is %s",
			strings.Replace(string(buff[:n]), "\n", "", -1))

		resp := Decode(buff[:n])
		switch resp.Cmd {
		case "pong":
			c.heartbeat = time.Now()
			continue
		case "invalid_msg":
			c.logger.Warnf("receive error message, error code %d, detail %s", resp.ErrorCode, resp.Message)
			continue
		default:
		}

		select {
		case c.receiveCh <- buff[:n]:
		default:
			lost := <-c.receiveCh
			c.logger.Errorf("failed to read message: queue is full")
			c.logger.Errorf("message is lost, the content is %s",
				strings.Replace(string(lost), "\n", "", -1))

			c.receiveCh <- buff[:n]
		}
	}
}

func (c *client) Write(buff []byte) {
	select {
	case c.sendCh <- buff:
	default:
		lost := <-c.sendCh
		c.logger.Errorf("failed to send message: queue is full")
		c.logger.Errorf("message is lost, the content is %s",
			strings.Replace(string(lost), "\n", "", -1))

		c.sendCh <- buff
	}
}

func (c *client) write(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Warnf("the connection has been closed, exiting the writing thread")
			return
		case buff := <-c.sendCh:
			if err := c.tlsConn.SetWriteDeadline(time.Now().Add(1 * time.Second)); err != nil {
				c.logger.Errorf("failed to set write deadline: %v", err)
				continue
			}
			_, err := c.tlsConn.Write(buff)
			if err != nil {
				c.logger.Errorf("failed to write message: %v, the content is %s", err,
					strings.Replace(string(buff), "\n", "", -1))
				continue
			}
			c.logger.Infof("write message successfully, the content is %s",
				strings.Replace(string(buff), "\n", "", -1))
		}
	}
}

func (c *client) login() error {
	req := LoginRequest{
		Cmd:           "login_req",
		PrefetchCount: 100,
		Data: []LoginRequestData{
			{
				ProductKey: c.settings.ProductKey,
				AuthID:     c.settings.AuthID,
				AuthSecret: c.settings.AuthSecret,
				SubKey:     c.settings.SubKey,
				Events:     defaultEventTypes,
			},
		},
	}
	if _, err := c.tlsConn.Write(Encode(req)); err != nil {
		return err
	}

	buff := make([]byte, 1024)
	n, err := c.tlsConn.Read(buff)
	if err != nil {
		return err
	}

	var resp LoginResponse
	if err = json.Unmarshal(buff[:n], &resp); err != nil {
		return err
	}
	if !resp.Data.Result {
		return errors.New(resp.Data.Message)
	}
	return nil
}

func (c *client) ping(ctx context.Context, cancel context.CancelFunc) bool {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	c.heartbeat = time.Now()
	for {
		select {
		case <-ctx.Done():
			c.logger.Warnf("the connection has been closed, exiting the ping thread")
			return false
		case <-c.shutdown:
			cancel()
			c.logger.Warnf("the connection has been closed, exiting the ping thread")
			return true
		case <-ticker.C:
			if time.Now().Sub(c.heartbeat) > time.Minute {
				cancel()
				continue
			}
			c.Write(Encode(PingRequest{Cmd: "ping"}))
		}
	}
}
