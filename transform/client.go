package transform

import "context"

type Client struct {
}

func New(ctx context.Context) *Client {
	return &Client{}
}

func (c *Client) Encode(ctx context.Context, rawMsgChan, cacheInChan, cacheOutChan, transformedMsgChan chan string) error {
	return nil
}

func (c *Client) Close() error {
	return nil
}
