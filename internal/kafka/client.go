package kafka

import (
	"context"

	"github.com/champ-isaac/privacy_api_service/pkgs/log"
)

type Client struct {
}

func New(ctx context.Context) *Client {
	return &Client{}
}

func (c *Client) Consume(ctx context.Context, sourceChan, rawMsgChan chan string) error {
	logger := log.LoggerFrom(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-sourceChan:
			if !ok {
				return nil
			}
			logger.Info("<-sourceChan", "msg", msg)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case rawMsgChan <- c.consumeMsg(ctx, msg):
				logger.Info("rawMsgChan<-", "msg", msg)
			}
		}
	}
}

func (c *Client) Produce(ctx context.Context, transformedMsgChan, resultChan chan string) error {
	logger := log.LoggerFrom(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-transformedMsgChan:
			if !ok {
				return nil
			}
			logger.Info("<-transformedMsgChan", "msg", msg)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case resultChan <- c.produceMsg(ctx, msg):
				logger.Info("resultChan<-", "msg", msg)
			}
		}
	}
}

func (c *Client) Close() error {
	return nil
}

func (c *Client) consumeMsg(ctx context.Context, msg string) string {
	//logger := log.LoggerFrom(ctx)
	//logger.Info("consume raw message", "msg", msg, "rawMsgChan", "<-")
	return msg
}

func (c *Client) produceMsg(ctx context.Context, msg string) string {
	//logger := log.LoggerFrom(ctx)
	//logger.Info("produce transformed message", "msg", msg, "resultChan", "<-")
	return msg
}
