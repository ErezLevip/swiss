package middlewares

import (
	"github.com/erezlevip/swiss/entities"
)

type MiddlewareWrapper struct {
	Chain []Middleware
}

type Middleware func(next func(ctx *entities.Context)) func(ctx *entities.Context)

func (mw *MiddlewareWrapper) Add(middlewares ...Middleware) *MiddlewareWrapper {
	for _, m := range middlewares {
		mw.Chain = append(mw.Chain, m)
	}
	return mw
}

func (md *MiddlewareWrapper) Then(handler func(ctx *entities.Context)) func(ctx *entities.Context) {
	maxIdx := len(md.Chain) - 1
	builtChain := handler
	for idx := range md.Chain {
		builtChain = md.Chain[maxIdx-idx](builtChain)
	}
	return builtChain
}
