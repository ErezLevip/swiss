package swiss

import (
	"errors"
	"fmt"
	"github.com/erezlevip/swiss/entities"
	"github.com/sirupsen/logrus"
)

type Router struct {
	handlers map[string]MessageHandlerFunc
}

func (ec *EventsConsumer) AddRouter() *Router {
	if ec.router != nil {
		return ec.router
	}

	ec.router = &Router{
		handlers: make(map[string]MessageHandlerFunc),
	}
	return ec.router
}

func (r *Router) route() MessageHandlerFunc {
	return func(mctx *entities.Context) {
		if msgHandler, exists := r.handlers[mctx.Topic]; exists {
			handlerWrapper(mctx, msgHandler)
			return
		}
		logrus.Errorf("handler %s not found", mctx.Topic)
	}
}

func handlerWrapper(ctx *entities.Context, handler MessageHandlerFunc) {
	defer func() {
		if err := recover(); err != nil {
			switch err.(type) {
			case string:
				ctx.Error(errors.New(err.(string)))
				return
			case error:
				ctx.Error(err.(error))
				return
			default:
				ctx.Error(fmt.Errorf("%v", err))
			}
		}
	}()

	handler(ctx)
}

func (r *Router) RegisterEndpoint(topic string, handler MessageHandlerFunc) *Router {
	if _, exists := r.handlers[topic]; exists {
		panic(fmt.Sprintf("%s aready exists", topic))
	}

	r.handlers[topic] = handler
	return r
}
