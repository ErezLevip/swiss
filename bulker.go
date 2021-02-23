package swiss

import (
	"context"
	"github.com/erezlevip/swiss/entities"
	"sync"
	"time"
)

type messagesBulker struct {
	maxIntervalTime time.Duration
	limit           int
	bulk            bulk
	m               sync.Mutex
	t               *time.Timer
}

type bulk struct {
	messages entities.Messages
}

func (b *messagesBulker) addOrTake(msg *entities.Message) (entities.Messages, bool) {
	b.m.Lock()
	defer b.m.Unlock()
	b.bulk.messages = append(b.bulk.messages, msg)

	if len(b.bulk.messages) >= b.limit {
		return b.takeUnsafe(), true
	}
	return nil, false
}

func (b *messagesBulker) takeUnsafe() entities.Messages {
	out := b.bulk.messages
	b.bulk.messages = make(entities.Messages, 0, b.limit)
	b.t.Reset(b.maxIntervalTime)
	return out
}

func (b *messagesBulker) takeSafe() entities.Messages {
	b.m.Lock()
	defer b.m.Unlock()
	return b.takeUnsafe()
}

func newMessageBulker(ctx context.Context, maxInterval time.Duration, limit int, flushFunc func(messages entities.Messages)) *messagesBulker {
	b := &messagesBulker{
		maxIntervalTime: maxInterval,
		limit:           limit,
	}
	if maxInterval <= 0 {
		maxInterval = time.Second
	}
	if limit <= 0 {
		limit = 1
	}
	b.t = time.NewTimer(maxInterval)

	if maxInterval > 0 {
		go runTicker(ctx, b, flushFunc)
	}
	return b
}

func runTicker(ctx context.Context, b *messagesBulker, flushFunc func(messages entities.Messages)) {
	for {
		select {
		case <-ctx.Done():
			b.t.Stop()
			return
		case <-b.t.C:
			flushFunc(b.takeSafe())
			b.t.Reset(b.maxIntervalTime)
		}
	}
}
