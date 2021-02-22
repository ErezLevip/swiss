package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	swiss "github.com/erezlevip/swiss"
	"github.com/erezlevip/swiss/configs"
	"github.com/erezlevip/swiss/entities"
	"github.com/erezlevip/swiss/listeners/kafka"
	"github.com/sirupsen/logrus"
	"os"
	"strings"
	"time"
)

func main() {
	kafkaCon := ""
	cfg := newListenerConfig("users12", kafkaCon, "")
	saramaConsumer := swiss.NewConsumer(kafka.NewListener(cfg))
	saramaConsumer.AddRouter().RegisterEndpoint("test-topic", createHandler())
	saramaConsumer.Run()
}

func init() {
	// Log as JSON instead of the default ASCII formatter.
	logrus.SetFormatter(&logrus.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	logrus.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	logrus.SetLevel(logrus.WarnLevel)
}

func newListenerConfig(consumerGroup, conStr, topic string) *kafka.Config {
	return &kafka.Config{
		Consumer: configs.Consumer{
			Verbose:         true,
			Connections:     strings.Split(conStr, ","),
			Topics:          strings.Split(topic, ","),
			MaxWaitTime:     3 * time.Minute,
			Concurrency:     100,
			BulkMaxInterval: time.Second,
			BulkSize:        100,
		},
		Offset:            sarama.OffsetOldest,
		ProcessingTimeout: time.Minute,
		Version:           "1.1.1",
		ConsumerGroup:     consumerGroup,
	}
}

func createHandler() func(ctx *entities.Context) {
	return func(ctx *entities.Context) {
		fmt.Println(ctx.Topic, "size", len(ctx.Bulk()))
		if err := ctx.Ack(); err != nil {
			fmt.Println(err)
		}
	}
}
