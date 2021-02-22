package handlers

import (
	"github.com/erezlevip/swiss/entities"
	"github.com/sirupsen/logrus"
)

type ErrorHandlerFunc func(ctx *entities.Context)

func HandleErrors(errors <-chan *entities.TopicError) {
	for err := range errors {
		logrus.Debugf ("topic %s, error %s\n", err.Topic, err.Error)
	}
}
