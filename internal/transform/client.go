package transform

import (
	"context"
	"log"
)

type Client struct {
}

func New(ctx context.Context) *Client {
	return &Client{}
}

func (c *Client) Encode(ctx context.Context, rawMsgChan, cacheInChan, cacheOutChan, transformedMsgChan chan string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-rawMsgChan:
			if !ok {
				return nil
			}
			log.Printf("received raw message: %s", msg)
			cacheInChan <- msg
		case msg, ok := <-cacheOutChan:
			if !ok {
				return nil
			}
			log.Printf("received cacheOut cached message: %s", msg)
			transformedMsg, err := c.transform(msg)
			if err != nil {
				return err
			}
			transformedMsgChan <- transformedMsg
		}
	}
}

func (c *Client) transform(msg string) (string, error) {
	transformedMsg := msg
	log.Printf("transformed message: %s", transformedMsg)
	return transformedMsg, nil
}

func (c *Client) Close() error {
	return nil
}
