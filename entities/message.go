package entities

type Messages []*Message

type Message struct {
	Value     []byte
	Topic     string
	Metadata  map[string]string
	Ack       func() error
	Partition int
	Offset    int64
	Id        string
}
