package events

import (
	"context"
	"github.com/dbacilio88/poc-rabbit-subscriber-go/internal/events/mq"
	"go.uber.org/zap"
)

/**
*
* broker
* <p>
* broker file
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

type Broker struct {
	*zap.Logger
	mq.IBrokerFactory
}

type IBroker interface {
	Subscriber(topic string)
}

func NewBroker(log *zap.Logger) *Broker {
	return &Broker{
		Logger: log,
	}
}

func (b *Broker) InitBroker(instance int) *Broker {
	b.Info("Initializing Broker", zap.Int("instance", instance))
	factory, err := mq.NewBrokerFactory(b.Logger, instance)
	if err != nil {
		b.Error("Error initializing Broker", zap.Error(err))
		return nil
	}
	b.IBrokerFactory = factory
	b.Info("Initializing Broker", zap.Int("instance", instance))
	return b
}

func (b *Broker) Subscriber() {
	b.Info("Starting Subscriber", zap.Int("instance", 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.IBrokerFactory.Subscriber(ctx)
	b.Info("Started Subscriber", zap.Int("instance", 0))

}
