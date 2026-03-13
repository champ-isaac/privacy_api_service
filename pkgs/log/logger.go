package log

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey struct{}

var (
	loggerKey     = &ctxKey{}
	defaultLogger *slog.Logger
	logLevel      = new(slog.LevelVar)
)

func init() {
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: false,
		Level:     logLevel,
	})
	defaultLogger = slog.New(jsonHandler)
	slog.SetDefault(defaultLogger)
}

func InitLogger(level slog.Level) *slog.Logger {
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: false,
		Level:     level,
	})
	logger := slog.New(jsonHandler)
	defaultLogger = logger

	return logger
}

func LoggerFrom(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return l
	}
	return defaultLogger
}

func ContextWithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

func SetLogLevel(level slog.Level) {
	logLevel.Set(level)
}
