package redis

import (
	"context"
	"time"

	"github.com/champ-isaac/privacy_api_service/pkgs/log"
)

type Client struct {
}

func New(ctx context.Context) *Client {
	return &Client{}
}

func (c *Client) Service(ctx context.Context, cacheInChan, cacheOutChan chan string) error {
	logger := log.LoggerFrom(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-cacheInChan:
			if !ok {
				return nil
			}
			logger.Info("<-cacheInChan", "msg", msg)
			cachedMsg := c.cacheIn(ctx, msg)
			time.Sleep(400 * time.Millisecond)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case cacheOutChan <- c.cacheOut(ctx, cachedMsg):
				logger.Info("cacheOutChan<-", "msg", cachedMsg)
			}
		}
	}
}

func (c *Client) cacheIn(ctx context.Context, msg string) string {
	//logger := log.LoggerFrom(ctx)
	//logger.Info("cacheIn raw message", "msg", msg, "<-", "cacheInChan")
	return msg
}

func (c *Client) cacheOut(ctx context.Context, msg string) string {
	//logger := log.LoggerFrom(ctx)
	//logger.Info("cacheOut cached message", "msg", msg, "cacheOutChan", "<-")
	return msg
}

func (c *Client) Close() error {
	return nil
}
