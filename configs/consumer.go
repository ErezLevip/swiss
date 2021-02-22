package configs

import "time"

type Consumer struct {
	Verbose         bool
	Connections     []string      //the connection strings, can support multiple in the case of sqs or kafka brokers
	Topics          []string      //topics collection
	MaxWaitTime     time.Duration //kafka max wait for messages
	Concurrency     int
	BulkSize        int
	BulkMaxInterval time.Duration
	MessageUnmarshaler
}
