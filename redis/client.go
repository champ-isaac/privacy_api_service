package redis

import "context"

type Client struct {
}

func New(ctx context.Context) *Client {
	return &Client{}
}

func (c *Client) Service(ctx context.Context, cacheInChan, cacheOutChan chan string) error {
	return nil
}

func (c *Client) Close() error {
	return nil
}
