package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/champ-isaac/privacy_api_service/kafka"
	"github.com/champ-isaac/privacy_api_service/redis"
	"github.com/champ-isaac/privacy_api_service/transform"
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
