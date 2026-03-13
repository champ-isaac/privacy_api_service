package transform

import (
	"context"

	"github.com/champ-isaac/privacy_api_service/pkgs/log"
)

type Client struct {
}

func New(ctx context.Context) *Client {
	return &Client{}
}

func (c *Client) Encode(ctx context.Context, rawMsgChan, cacheInChan, cacheOutChan, transformedMsgChan chan string) error {
	logger := log.LoggerFrom(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-rawMsgChan:
			if !ok {
				return nil
			}
			logger.Info("<-rawMsgChan", "msg", msg)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case cacheInChan <- msg:
				logger.Info("cacheInChan<-", "msg", msg)
			}
		case msg, ok := <-cacheOutChan:
			if !ok {
				return nil
			}
			logger.Info("<-cacheOutChan", "msg", msg)
			transformedMsg, err := c.transform(ctx, msg)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case transformedMsgChan <- transformedMsg:
				logger.Info("transformedMsgChan<-", "msg", msg)
			}
		}
	}
}

func (c *Client) transform(ctx context.Context, msg string) (string, error) {
	//logger := log.LoggerFrom(ctx)
	transformedMsg := msg
	//logger.Info("transformed message", "msg", transformedMsg)
	return transformedMsg, nil
}

func (c *Client) Close() error {
	return nil
}
