package redis

import (
	"context"
	"log"
	"time"
)

type Client struct {
}

func New(ctx context.Context) *Client {
	return &Client{}
}

func (c *Client) Service(ctx context.Context, cacheInChan, cacheOutChan chan string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-cacheInChan:
			if !ok {
				return nil
			}
			cachedMsg := c.cacheIn(msg)
			time.Sleep(400 * time.Millisecond)
			cacheOutChan <- c.cacheOut(cachedMsg)
		}
	}
}

func (c *Client) cacheIn(msg string) string {
	log.Printf("cacheIn raw message %s", msg)
	return msg
}

func (c *Client) cacheOut(msg string) string {
	log.Printf("cacheOut cached message %s", msg)
	return msg
}

func (c *Client) Close() error {
	return nil
}
