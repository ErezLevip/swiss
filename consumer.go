package swiss

import (
	"context"
	"fmt"
	"github.com/erezlevip/swiss/entities"
	"github.com/erezlevip/swiss/handlers"
	"github.com/erezlevip/swiss/listeners"
	"github.com/erezlevip/swiss/middlewares"
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

type Consumer interface {
	Run()
	Stop()
	AddMiddlewareChain() *middlewares.MiddlewareWrapper
	AddRouter() *Router
}

type MessageHandlerFunc func(ctx *entities.Context)

type EventsConsumer struct {
	listener         listeners.EventListener
	router           *Router
	chain            *middlewares.MiddlewareWrapper
	recoverFromPanic bool
	stop             context.CancelFunc
}

func (ec *EventsConsumer) Stop() {
	ec.stop()
}

func NewConsumer(listener listeners.EventListener) Consumer {
	return &EventsConsumer{
		listener:         listener,
		recoverFromPanic: true,
	}
}

func (ec *EventsConsumer) Run() {
	loggingMiddleware := middlewares.NewLoggingMiddleware()
	executionCtx := &executionContext{
		chainStart: ec.AddMiddlewareChain().Add(loggingMiddleware.Logging).Then(ec.router.route()),
		acker:      ec.listener.Acker(),
	}

	executionCtx.Context, ec.stop = context.WithCancel(context.Background())
	executionCtx.out, executionCtx.errors = ec.listener.Listen(executionCtx)

	killSwitch := make(chan os.Signal)
	signal.Notify(killSwitch, syscall.SIGTERM, os.Interrupt)

	ec.executeListener(executionCtx)

	go func() {
		<-killSwitch
		ec.stop()
	}()

	<-executionCtx.Done()
	for _, t := range ec.listener.ConsumerConfig().Topics {
		logrus.Debugf("interrupt, topic %s", t)
	}

	logrus.Fatal("interrupt, shutting down")
}

func (ec *EventsConsumer) executeListener(ctx *executionContext) {
	logrus.Info("start listening")
	for t, c := range ctx.out {
		go ec.handleTopic(t, c, ctx)
	}

	go handlers.HandleErrors(ctx.errors)
}

func (ec *EventsConsumer) handleTopic(t string, c listeners.ConsumerOutChannel, execContext *executionContext) {
	concurrency := ec.listener.ConsumerConfig().Concurrency
	sem := make(chan struct{}, concurrency)
	flushFunc := func(bulk entities.Messages) {
		ec.handleBulk(bulk, execContext, t)
	}
	bulker := newMessageBulker(execContext, ec.listener.ConsumerConfig().BulkMaxInterval, ec.listener.ConsumerConfig().BulkSize, flushFunc)

	for {
		select {
		case m, ok := <-c:
			if !ok {
				return
			}

			sem <- struct{}{}
			go func(m *entities.Message) {
				defer func() {
					<-sem
				}()

				if bulk, ready := bulker.addOrTake(m); ready {
					ec.handleBulk(bulk, execContext, t)
				}
			}(m)

		case <-execContext.Done():
			return
		}
	}
}

func (ec *EventsConsumer) handleBulk(messages entities.Messages, execContext *executionContext, topic string) {
	if ec.recoverFromPanic {
		defer func() {
			if err := recover(); err != nil {
				fmt.Println(err)
			}
		}()
	}

	execContext.chainStart(entities.NewMessageContext(messages, topic, execContext.Context, execContext.acker))
}

func (ec *EventsConsumer) AddMiddlewareChain() *middlewares.MiddlewareWrapper {
	if ec.chain == nil {
		ec.chain = &middlewares.MiddlewareWrapper{
			Chain: make([]middlewares.Middleware, 0),
		}
	}
	return ec.chain
}
