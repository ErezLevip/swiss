package sqs

import (
	"context"
	"github.com/erezlevip/swiss/configs"
	"github.com/erezlevip/swiss/entities"
	"github.com/erezlevip/swiss/listeners"
	"github.com/google/uuid"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type sqsListener struct {
	config   *Sqs
	sessions map[string]*sqs.SQS
}

func (l *sqsListener) Acker() listeners.Acker {
	return nil
}

func NewListener(config *Sqs) listeners.EventListener {
	config.fillDefaults()
	return &sqsListener{
		config:   config,
		sessions: make(map[string]*sqs.SQS),
	}
}

func (l *sqsListener) ConsumerConfig() *configs.Consumer {
	return &l.config.Consumer
}

func (l *sqsListener) getSession(region string) *sqs.SQS {
	if s, ok := l.sessions[region]; ok {
		return s
	}

	awsConfig := aws.NewConfig().WithRegion(region)
	if len(l.config.ServiceEndpoint) > 0 {
		// If we specify a service endpoint then we use it (allows for local development)
		awsConfig.WithEndpoint(l.config.ServiceEndpoint)
	}

	sess := session.Must(session.NewSession(awsConfig))

	sqsSession := sqs.New(sess)
	l.sessions[region] = sqsSession
	return sqsSession
}

func (l *sqsListener) CommitOnCompletion() bool {
	return l.config.CommitOnCompletion
}

func (l *sqsListener) Listen(ctx context.Context) (listeners.TopicMessages, <-chan *entities.TopicError) {
	outMap := make(listeners.TopicMessages, len(l.config.Topics))
	errors := make(chan *entities.TopicError)

	svc := l.getSession(l.config.AwsRegion)

	for _, t := range l.config.Topics {
		outMap[t] = make(chan *entities.Message, l.config.MaxBufferSize)
	}

	for _, connection := range l.config.Connections {
		outChan, errChan := l.consume(l.config.Topics, connection, svc, ctx)

		go func() {
			for msg := range outChan {
				if h, ok := outMap[msg.Topic]; ok {
					h <- msg
				}
			}
		}()
		go func() {
			for err := range errChan {
				errors <- err
			}
		}()
	}

	return outMap, errors
}

func (l *sqsListener) consume(topics []string, url string, sq *sqs.SQS, ctx context.Context) (chan *entities.Message, chan *entities.TopicError) {
	tm := topicsToMap(topics)
	out := make(chan *entities.Message, l.config.MaxBufferSize)
	errs := make(chan *entities.TopicError)

	go func() {
		defer close(out)
		acker := newSqsAcker(sq, aws.String(url), errs)
		topic := getTopicFromUrl(url)
		for {
			msgs, err := sq.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(url),
				MaxNumberOfMessages: aws.Int64(int64(l.config.MaxMessagesPull)),
				WaitTimeSeconds:     aws.Int64(l.config.MaxWaitTime.Milliseconds()),
			})
			if err != nil {
				errs <- entities.NewTopicError(url, err)
				continue
			}

			for _, msg := range msgs.Messages {
				if _, ok := tm[topic]; !ok {
					continue
				}

				out <- &entities.Message{
					Topic:    topic,
					Metadata: map[string]string{},
					Value:    []byte(*msg.Body),
					Ack: func() error {
						acker.akc(msg.ReceiptHandle)
						return nil
					},
					Id: uuid.NewString(),
				}

				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		}
	}()

	return out, errs
}

func topicsToMap(topics []string) map[string]struct{} {
	tm := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		tm[t] = struct{}{}
	}
	return tm
}

func getTopicFromUrl(url string) string {
	split := strings.Split(url, "/")
	return split[len(split)-1]
}

type sqsAcker struct {
	sqs   *sqs.SQS
	errs  chan *entities.TopicError
	queue *string

	buff []*sqs.DeleteMessageBatchRequestEntry

	mu sync.Mutex
}

func newSqsAcker(s *sqs.SQS, queue *string, errs chan *entities.TopicError) *sqsAcker {
	sa := &sqsAcker{
		sqs:   s,
		errs:  errs,
		queue: queue,
		buff:  make([]*sqs.DeleteMessageBatchRequestEntry, 0, 10),
		mu:    sync.Mutex{},
	}
	go sa.flush()
	return sa
}

func (s *sqsAcker) flush() {
	t := time.NewTicker(time.Second)
	for range t.C {
		go s.send(true)
	}
}

func (s *sqsAcker) send(shouldLock bool) {
	if shouldLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	if len(s.buff) == 0 {
		return
	}
	tmp := s.buff
	s.buff = make([]*sqs.DeleteMessageBatchRequestEntry, 0, 10)
	go func() {
		if _, err := s.sqs.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
			Entries:  tmp,
			QueueUrl: s.queue,
		}); err != nil {
			s.errs <- entities.NewTopicError(*s.queue, err)
		}
	}()
}

func (s *sqsAcker) akc(id *string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buff = append(s.buff, &sqs.DeleteMessageBatchRequestEntry{
		Id:            aws.String(uuid.New().String()),
		ReceiptHandle: id,
	})
	if len(s.buff) == 10 {
		s.send(false)
	}
}
