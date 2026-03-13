package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/champ-isaac/privacy_api_service/internal/kafka"
	"github.com/champ-isaac/privacy_api_service/internal/redis"
	"github.com/champ-isaac/privacy_api_service/internal/transform"
	"github.com/champ-isaac/privacy_api_service/pkgs/log"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	logger := log.LoggerFrom(ctx)
	log.SetLogLevel(slog.LevelDebug)

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
		defer close(sourceChan)
		return generator(ctx, sourceChan)
	})

	g.Go(func() error {
		defer close(rawMsgChan)
		return kafkaClient.Consume(ctx, sourceChan, rawMsgChan)
	})

	g.Go(func() error {
		defer close(cacheOutChan)
		defer close(cacheInChan)
		return redisClient.Service(ctx, cacheInChan, cacheOutChan)
	})

	g.Go(func() error {
		defer close(transformedMsgChan)
		return transformClient.Encode(ctx, rawMsgChan, cacheInChan, cacheOutChan, transformedMsgChan)
	})

	g.Go(func() error {
		defer close(resultChan)
		return kafkaClient.Produce(ctx, transformedMsgChan, resultChan)
	})

	g.Go(func() error {
		return result(ctx, resultChan)
	})
	//stop pipelines
	go func() {
		signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

		sigMsg := <-signalChan
		logger.Info("received signal message", "sigMsg", sigMsg)

		cancel()

		//close service components
		_ = kafkaClient.Close()
		_ = redisClient.Close()
		_ = transformClient.Close()
	}()

	if err := g.Wait(); err != nil {
		logger.Error("pipeline stopped: ", "error", err)
	}
}

func generator(ctx context.Context, sourceChan chan string) error {
	logger := log.LoggerFrom(ctx)
	i := 1
	for {
		msg := fmt.Sprintf("generate [message%06d]", i)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sourceChan <- fmt.Sprintf("[message%06d]", i):
			logger.Info("sourceChan<-", "msg", msg)
		}
		time.Sleep(500 * time.Millisecond)
		i++
	}
}

func result(ctx context.Context, resultChan chan string) error {
	logger := log.LoggerFrom(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-resultChan:
			if !ok {
				return nil
			}
			logger.Info("<-resultChan", "msg", msg)
		}
	}
}
