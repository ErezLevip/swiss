package entities

import (
	"context"
	"errors"
	"github.com/erezlevip/swiss/configs"
	"github.com/sirupsen/logrus"
)

type Context struct {
	context.Context
	configs.MessageUnmarshaler
	messages Messages
	acker    Acker
	Topic    string
}

type Acker interface {
	Ack(messages Messages) error
}

func NewMessageContext(messages Messages, topic string, ctx context.Context, acker Acker) *Context {
	return &Context{
		messages: messages,
		Context:  ctx,
		Topic:    topic,
		acker:    acker,
	}
}

func (ctx *Context) Bulk() Messages {
	return ctx.messages
}

func (ctx *Context) Message() (*Message, error) {
	if len(ctx.messages) > 1 {
		return nil, errors.New("single message consumed in bulk mode")
	}
	return ctx.messages[0], nil
}

func (ctx *Context) Ack() error {
	if ctx.acker != nil {
		return ctx.acker.Ack(ctx.messages)
	}

	for _, msg := range ctx.Bulk() {
		if err := msg.Ack(); err != nil {
			return err
		}
	}
	return nil
}

func (ctx *Context) Unmarshal(msg *Message, v interface{}) error {
	return ctx.MessageUnmarshaler.Unmarshal(msg.Value, v)
}

func (ctx *Context) Error(err error) {
	if err == nil {
		return
	}
	logrus.Error(err)
}

func (ctx *Context) ErrorWithContext(err error, executionContext, function string) {
	if err == nil {
		return
	}

	logrus.Error(err, "context", executionContext, "function", function)
}
