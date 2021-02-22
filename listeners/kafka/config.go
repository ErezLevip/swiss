package kafka

import (
	"crypto/tls"
	"fmt"
	"github.com/erezlevip/swiss/configs"
	"github.com/google/uuid"
	"github.com/Shopify/sarama"
	"time"
)

type Config struct {
	configs.Consumer
	ConsumerGroup           string        //used only on kafka, specify the name of the consumer group
	Offset                  int64         //used only on kafka,  specify the initial offset (oldest,newest, or specific)
	ProcessingTimeout       time.Duration //specify the maximum amount of time for processing a message
	Tls                     *tls.Config   //kafka tls config
	Version                 string        //the version of the kafka server
	MaxWaitTimeMilliseconds time.Duration //kafka max wait for messages
	LagRetentionTime        time.Duration //kafka lag retention
	MaxBufferSize           int           //the amount of messages to pull before processing, 1 for kafka
}


func (cfg *Config) validateAndApplyConfigDefaults() error {
	if cfg == nil {
		return fmt.Errorf("cfg cant be nil")
	}

	if cfg.ProcessingTimeout == 0 {
		cfg.ProcessingTimeout = 10 * time.Second
	}

	if cfg.Offset == 0 {
		cfg.Offset = sarama.OffsetOldest
	}

	if len(cfg.Topics) == 0 {
		return fmt.Errorf("topics are missing")
	}

	if len(cfg.Connections) == 0 {
		return fmt.Errorf("connection is missing")
	}

	if cfg.Version == "" {
		return fmt.Errorf("kafka version camt be empty")
	}
	if cfg.MaxBufferSize < 1 {
		cfg.MaxBufferSize = 1
	}

	return nil
}

func (cfg *Config) ToSaramaConfig() (*sarama.Config, error) {
	var config = sarama.NewConfig()
	config.Net.TLS.Config = cfg.Tls
	config.Net.TLS.Enable = cfg.Tls != nil
	config.Net.DialTimeout = time.Minute
	config.Consumer.Return.Errors = true
	config.Consumer.MaxProcessingTime = cfg.ProcessingTimeout
	config.Consumer.Offsets.Initial = cfg.Offset
	config.ChannelBufferSize = cfg.MaxBufferSize
	config.Consumer.Group.Session.Timeout = 60 * time.Second
	config.Consumer.Group.Rebalance.Retry.Max = 10
	config.ClientID = uuid.New().String()

	if cfg.LagRetentionTime > 0 {
		config.Consumer.Offsets.Retention = cfg.LagRetentionTime
	}

	if cfg.MaxWaitTimeMilliseconds > 0 {
		config.Consumer.MaxWaitTime = cfg.MaxWaitTimeMilliseconds
	}

	var err error
	config.Version, err = sarama.ParseKafkaVersion(cfg.Version)
	return config, err
}
