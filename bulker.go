package swiss

import (
	"context"
	"github.com/erezlevip/swiss/entities"
	"sync"
	"time"
)

type MessagesBulker struct {
	maxIntervalTime time.Duration
	limit           int
	bulk            bulk
	m               sync.Mutex
	t               *time.Timer
}

type bulk struct {
	messages entities.Messages
}

func (b *MessagesBulker) Add(msg *entities.Message) bool {
	b.m.Lock()
	defer b.m.Unlock()

	if b.bulk.messages == nil || len(b.bulk.messages) == b.limit {
		b.bulk.messages = make(entities.Messages, 0, b.limit)
		b.t.Reset(b.maxIntervalTime)
	}

	b.bulk.messages = append(b.bulk.messages, msg)
	return len(b.bulk.messages) >= b.limit
}

func (b *MessagesBulker) Take(flushAll bool) (entities.Messages, bool) {
	b.m.Lock()
	defer b.m.Unlock()
	if len(b.bulk.messages) >= b.limit || (len(b.bulk.messages) > 0 && flushAll) {
		out := b.bulk.messages
		b.bulk.messages = make(entities.Messages, 0, b.limit)
		b.t.Reset(b.maxIntervalTime)
		return out, true
	}
	return nil, false
}

func NewMessageBulker(ctx context.Context, maxInterval time.Duration, limit int, flushFunc func(messages entities.Messages)) *MessagesBulker {
	b := &MessagesBulker{
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

func runTicker(ctx context.Context, b *MessagesBulker, flushFunc func(messages entities.Messages)) {
	for {
		select {
		case <-ctx.Done():
			b.t.Stop()
			return
		case <-b.t.C:
			messages, ok := b.Take(true)
			if ok {
				flushFunc(messages)
				b.t.Reset(b.maxIntervalTime)
			}
		}
	}
}
