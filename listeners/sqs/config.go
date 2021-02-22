package sqs

import (
	"github.com/erezlevip/swiss/configs"
	"time"
)

type Sqs struct {
	configs.Consumer
	MaxBufferSize           int           //the amount of messages to pull before processing, 1 for kafka
	ProcessingTimeout       time.Duration //specify the maximum amount of time for processing a message
	CommitOnCompletion   bool          //auto commit once a message is pulled default is false
	AwsRegion               string        //aws region
	TopicFromBody           bool          //in case of topics with base event structures the topic can be fetched from the body
	Verbose                 bool          //verbose mode
	Concurrency             int           //concurrency level, 1 for kafka, sqs can be up to 10
	ServiceEndpoint         string
	MaxMessagesPull         int
	OnDedupeFailureContinue bool
}

func (config *Sqs) fillDefaults() {
	if config.MaxBufferSize < 1 {
		config.MaxBufferSize = 1
	}

	if config.MaxWaitTime< 1000 {
		config.MaxWaitTime = 1000
	}

	if config.Concurrency <= 0 {
		config.Concurrency = 10
	}

	if config.ProcessingTimeout < time.Second*1 {
		config.ProcessingTimeout = time.Minute * 1
	}

	if config.MaxMessagesPull < 1 {
		config.MaxMessagesPull = 10
	}

	if config.MaxMessagesPull > 10 {
		// SQS can only take a maximum of 10 messages so MaxMessagesPull should not exceed that
		config.MaxMessagesPull = 10
	}
}