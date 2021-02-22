package middlewares

import (
	"github.com/erezlevip/swiss/entities"
	"github.com/sirupsen/logrus"
)

type LoggingMiddleware struct {
}

func NewLoggingMiddleware() *LoggingMiddleware {
	return &LoggingMiddleware{
	}
}

const (
	debugIncomingMessageFormat            = "incoming message %s on topic %s"
	debugProcessingCompletedMessageFormat = "processing completed for message %s on topic %s"
)

func (m *LoggingMiddleware) Logging(next func(ctx *entities.Context)) func(ctx *entities.Context) {
	return func(ctx *entities.Context) {
		for _, msg := range ctx.Bulk() {
			logrus.Debugf(debugIncomingMessageFormat, msg.Id, msg.Topic)
			next(ctx)
			logrus.Debugf(debugProcessingCompletedMessageFormat, msg.Id, msg.Topic)
		}
	}
}
