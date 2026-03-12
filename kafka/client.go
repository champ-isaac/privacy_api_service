package kafka

import "context"

type Client struct {
}

func New(ctx context.Context) *Client {
	return &Client{}
}

func (c *Client) Consume(ctx context.Context, sourceChan, rawMsgChan chan string) error {
	return nil
}

func (c *Client) Produce(ctx context.Context, transformedMsgChan, resultChan chan string) error {
	return nil
}

func (c *Client) Close() error {
	return nil
}
