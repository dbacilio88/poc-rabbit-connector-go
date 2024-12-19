package mq

import (
	"context"
	"errors"
	"go.uber.org/zap"
)

/**
*
* factory
* <p>
* factory file
*
* Copyright (c) 2024 All rights reserved.
*
* This source code is shared under a collaborative license.
* Contributions, suggestions, and improvements are welcome!
* Feel free to fork, modify, and submit pull requests under the terms of the repository's license.
* Please ensure proper attribution to the original author(s) and maintain this notice in derivative works.
*
* @author christian
* @author dbacilio88@outlook.es
* @since 18/12/2024
*
 */

const RabbitMqInstance int = iota
const KafkaMqInstance = 1

type BrokerFactory struct {
}

type IBrokerFactory interface {
	LoadConfiguration()
	Subscriber(ctx context.Context)
	Publisher()
}

func NewBrokerFactory(log *zap.Logger, instance int) (IBrokerFactory, error) {
	switch instance {
	case RabbitMqInstance:
		log.Info("initializing RabbitMq instance")
		return NewRabbitMQ(log), nil
	case KafkaMqInstance:
		log.Info("initializing Kafka instance")
		return nil, errors.New("kafka no implementado aún")
	default:
		return nil, errors.New("invalid instance")
	}
}
