package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/erezlevip/swiss/entities"
	"strconv"
)

const MetadataKeyPartition = "partition"
const MetadataKeyOffset = "offset"

type consumerGroupHandler struct {
	ready  chan bool
	topics []string
	out    chan *entities.Message
	id     string
}

func newConsumerGroupHandler(clientId string) *consumerGroupHandler {
	return &consumerGroupHandler{
		id:    clientId,
		ready: make(chan bool, 1),
		out:   make(chan *entities.Message),
	}
}

func (cg *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(cg.ready)
	return nil
}

func (cg *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	cg.ready = make(chan bool, 1)
	return nil
}

func (cg *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, canRead := <-claim.Messages():
			if !canRead {
				return nil
			}
			cg.pushMessage(msg, session, claim)
			break
		}
	}
}

func (cg *consumerGroupHandler) pushMessage(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) {
	cg.out <- &entities.Message{
		Value: msg.Value,
		Topic: msg.Topic,
		Metadata: map[string]string{
			MetadataKeyPartition: strconv.Itoa(int(msg.Partition)),
			MetadataKeyOffset:    strconv.FormatInt(msg.Offset, 10),
		},
		Offset: msg.Offset,
		Ack: func() error {
			session.MarkOffset(msg.Topic, msg.Partition, msg.Offset, "")
			return nil
		},
		Partition: int(claim.Partition()),
		Id:        string(msg.Key),
	}
}
