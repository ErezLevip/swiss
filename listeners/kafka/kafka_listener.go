package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/erezlevip/swiss/configs"
	"github.com/erezlevip/swiss/entities"
	"github.com/erezlevip/swiss/listeners"
	"github.com/sirupsen/logrus"
)

type listener struct {
	config *Config
}

func (l *listener) Acker() entities.Acker {
	return l
}

type offsetAck struct {
	offset int64
	ack    func() error
}

func (l *listener) Ack(messages entities.Messages) error {
	partitionOffsetAck := make(map[int]*offsetAck, len(messages))
	for _, m := range messages {
		if _, ok := partitionOffsetAck[m.Partition]; !ok {
			partitionOffsetAck[m.Partition] = &offsetAck{
				offset: m.Offset,
				ack:    m.Ack,
			}
			continue
		}

		if m.Offset > partitionOffsetAck[m.Partition].offset {
			partitionOffsetAck[m.Partition].ack = m.Ack
		}
	}

	for _, oa := range partitionOffsetAck {
		if err := oa.ack(); err != nil {
			return err
		}
	}

	return nil
}

func NewListener(config *Config) listeners.EventListener {
	if err := config.validateAndApplyConfigDefaults(); err != nil {
		panic(err)
	}
	return &listener{
		config: config,
	}
}

func (l *listener) ConsumerConfig() *configs.Consumer {
	return &l.config.Consumer
}

func (l *listener) Listen(ctx context.Context) (listeners.TopicMessages, <-chan *entities.TopicError) {
	outMap := make(listeners.TopicMessages, len(l.config.Topics))

	config, err := l.config.ToSaramaConfig()
	if err != nil {
		panic(err)
	}

	if l.config.Verbose {
		sarama.Logger = logrus.StandardLogger()
	}

	cg, err := sarama.NewConsumerGroup(l.config.Connections, l.config.ConsumerGroup, config)
	if err != nil {
		panic(err)
	}

	for _, t := range l.config.Topics {
		outMap[t] = make(chan *entities.Message)
	}

	go l.consume(l.config.Topics, cg, ctx, config.ClientID, outMap)
	errChan := make(chan *entities.TopicError)
	go func() {
		for err := range cg.Errors() {
			errChan <- entities.NewTopicError("global", err)
		}
	}()

	return outMap, errChan
}

func (l *listener) consume(topics []string, cg sarama.ConsumerGroup, ctx context.Context, clientId string, outMap listeners.TopicMessages) {
	//out  will always stay open
	cgh := newConsumerGroupHandler(clientId)
	outCtx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			select {
			case val, canRead := <-cgh.out:
				if !canRead {
					return
				}
				if ch, ok := outMap[val.Topic]; ok {
					ch <- val
				}
			case <-outCtx.Done():
				return
			}
		}
	}()

	//consume forever, if the session ended restart
	for {
		if err := cg.Consume(ctx, topics, cgh); err != nil {
			if l.config.Verbose {
				fmt.Println(err)
			}
		}

		select {
		case <-ctx.Done():
			cancel()
			return
		default:
		}
	}
}
