package listeners

import (
	"context"
	"github.com/erezlevip/swiss/configs"
	"github.com/erezlevip/swiss/entities"
)

type EventListener interface {
	Listen(context.Context) (out TopicMessages, errors <-chan *entities.TopicError)
	ConsumerConfig() *configs.Consumer
	Acker() entities.Acker
}

type ConsumerOutChannel chan *entities.Message
type ConsumerErrorChannel chan *entities.TopicError
type TopicMessages map[string]ConsumerOutChannel
type TopicErrors map[string]ConsumerErrorChannel
