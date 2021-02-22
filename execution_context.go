package swiss

import (
	"context"
	"github.com/erezlevip/swiss/entities"
	"github.com/erezlevip/swiss/listeners"
)

type executionContext struct {
	out           listeners.TopicMessages
	errors        <- chan *entities.TopicError
	chainStart    func(ctx *entities.Context)
	context.Context
	acker entities.Acker
}
