package kafka

import (
	"context"
	"log"
)

type Client struct {
}

func New(ctx context.Context) *Client {
	return &Client{}
}

func (c *Client) Consume(ctx context.Context, sourceChan, rawMsgChan chan string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-sourceChan:
			if !ok {
				return nil
			}
			rawMsgChan <- c.consumeMsg(msg)
		}
	}
}

func (c *Client) Produce(ctx context.Context, transformedMsgChan, resultChan chan string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-transformedMsgChan:
			if !ok {
				return nil
			}
			resultChan <- c.produceMsg(msg)
		}
	}
}

func (c *Client) Close() error {
	return nil
}

func (c *Client) consumeMsg(msg string) string {
	log.Printf("consume raw message %s", msg)
	return msg
}

func (c *Client) produceMsg(msg string) string {
	log.Printf("produce transformed message %s", msg)
	return msg
}
