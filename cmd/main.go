package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/champ-isaac/privacy_api_service/internal/kafka"
	"github.com/champ-isaac/privacy_api_service/internal/redis"
	"github.com/champ-isaac/privacy_api_service/internal/transform"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	//initialize channels
	sourceChan := make(chan string)
	rawMsgChan := make(chan string)
	cacheInChan := make(chan string)
	cacheOutChan := make(chan string)
	transformedMsgChan := make(chan string)
	resultChan := make(chan string)
	signalChan := make(chan os.Signal, 1)

	//initialize service components
	kafkaClient := kafka.New(ctx)
	redisClient := redis.New(ctx)
	transformClient := transform.New(ctx)

	//build pipelines
	g.Go(func() error {
		generator(sourceChan)
		return nil
	})

	g.Go(func() error {
		return kafkaClient.Consume(ctx, sourceChan, rawMsgChan)
	})

	g.Go(func() error {
		return redisClient.Service(ctx, cacheInChan, cacheOutChan)
	})

	g.Go(func() error {
		return transformClient.Encode(ctx, rawMsgChan, cacheInChan, cacheOutChan, transformedMsgChan)
	})

	g.Go(func() error {
		return kafkaClient.Produce(ctx, transformedMsgChan, resultChan)
	})

	g.Go(func() error {
		return result(ctx, resultChan)
	})
	//stop pipelines
	go func() {
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

		<-signalChan
		cancel()

		//close channels
		close(sourceChan)
		close(rawMsgChan)
		close(cacheOutChan)
		close(cacheInChan)
		close(transformedMsgChan)
		close(resultChan)

		//close service components
		_ = kafkaClient.Close()
		_ = redisClient.Close()
		_ = transformClient.Close()
	}()

	if err := g.Wait(); err != nil {
		log.Println("pipeline stopped: ", err)
	}
}

func generator(sourceChan chan string) {
	i := 1
	for {
		log.Printf("generate message %06d", i)
		sourceChan <- fmt.Sprintf("message%06d", i)
		time.Sleep(500 * time.Millisecond)
		i++
	}
}

func result(ctx context.Context, resultChan chan string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-resultChan:
			if !ok {
				return nil
			}
			fmt.Printf("received message %s", msg)
		}
	}
}
