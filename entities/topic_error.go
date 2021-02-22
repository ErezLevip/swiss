package entities

type TopicError struct {
	Topic string
	Error error
}

func NewTopicError(topic string, err error) *TopicError {
	return &TopicError{
		Topic: topic,
		Error: err,
	}
}
