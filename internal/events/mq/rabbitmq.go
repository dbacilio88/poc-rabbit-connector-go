package mq

import (
	"context"
	"crypto/tls"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/dbacilio88/poc-rabbit-subscriber-go/internal/events/sub"
	"github.com/dbacilio88/poc-rabbit-subscriber-go/pkg/env"
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

/**
*
* rabbitmq
* <p>
* rabbitmq file
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

type RabbitMQ struct {
	amqp.Config
	*zap.Logger
	env.IParameterBroker
	subscriber sub.IBrokerSubscriber
}

type IRabbitMQ interface {
	SubscriberRabbitMQ(ctx context.Context, topic string) (<-chan *message.Message, error)
	PublisherRabbitMQ(topic string, data []byte) error
}

func NewRabbitMQ(log *zap.Logger) *RabbitMQ {
	return &RabbitMQ{
		Logger:     log,
		subscriber: sub.NewBrokerSubscriber(log),
	}
}

func (r *RabbitMQ) SubscriberRabbitMQ(ctx context.Context, topic string) (<-chan *message.Message, error) {
	r.Info("Subscribing to RabbitMQ", zap.String("topic", topic))
	mq, err := r.subscriber.SubscriberRabbitMq(r.Config)
	if err != nil {
		r.Error("Failed to subscribe to RabbitMQ", zap.Error(err))
		return nil, err
	}
	subscribe, err := mq.Subscribe(ctx, topic)
	if err != nil {
		r.Error("Failed to subscribe to RabbitMQ", zap.Error(err))
		return nil, err
	}
	return subscribe, nil
}

func (r *RabbitMQ) PublisherRabbitMQ(topic string, data []byte) error {
	return nil
}

func (r *RabbitMQ) Publisher() {
	r.Info("implement me")
}

func (r *RabbitMQ) Subscriber(ctx context.Context) {

	r.LoadConfiguration()

	mq, err := r.SubscriberRabbitMQ(ctx, "topic")
	if err != nil {
		r.Error("Failed to subscribe to topic", zap.Error(err))
		return
	}
	go func() {
		for msg := range mq {
			r.Info("Received message", zap.Any("message", msg))
			msg.Ack()
		}
	}()
	r.Info("Subscribed to topic")
}

func (r *RabbitMQ) LoadConfiguration() {
	r.Info("Loading configuration for RabbitMq...")
	cfg := amqp.Config{
		Connection: r.loadConnectionConfig(),
		//Exchange:        r.loadExchangeConfig(),
		//Queue:           r.loadQueueConfig(),
		//QueueBind:       r.loadQueueBindConfig(),
		//Marshaler:       amqp.DefaultMarshaler{},
		//TopologyBuilder: &amqp.DefaultTopologyBuilder{},
		//Publish:         r.loadPublishConfig(),
		//Consume:         r.loadConsumeConfig(),
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: true,
	}
	cfg.Connection.TLSConfig = tlsCfg

	r.Config = cfg
	r.Info("Configuration Loaded")
}

func (r *RabbitMQ) loadConnectionConfig() amqp.ConnectionConfig {
	//fmt.Println(r.GetUri())
	r.Info("Loading connection config...")
	return amqp.ConnectionConfig{
		AmqpURI: r.GetUri(),
		AmqpConfig: &amqp091.Config{
			Vhost: r.GetVhost(),
		},
		Reconnect: amqp.DefaultReconnectConfig(),
	}
}

func (r *RabbitMQ) loadExchangeConfig() amqp.ExchangeConfig {
	r.Info("Loading exchange config...")
	return amqp.ExchangeConfig{
		GenerateName: func(topic string) string {
			return r.GetExchange()
		},
		Type:    "topic",
		Durable: true,
		//AutoDeleted: false,
		//Internal:    false,
		//NoWait:      true,
		/*
			Arguments: map[string]interface{}{
				"alternative-exchange": "alt-exchange",
			},

		*/
	}
}

func (r *RabbitMQ) loadQueueConfig() amqp.QueueConfig {
	r.Info("Loading queue config...")
	return amqp.QueueConfig{
		GenerateName: func(topic string) string {
			return r.GetQueueName()
		},
		Durable: true,
		//Exclusive:  false,
		//NoWait:     true,
		AutoDelete: false,
		Arguments: map[string]interface{}{
			"x-message-ttl": 6000,
			"x-queue-type":  "quorum",
		},
	}
}

func (r *RabbitMQ) loadQueueBindConfig() amqp.QueueBindConfig {
	r.Info("Loading queue binding config...")
	return amqp.QueueBindConfig{
		GenerateRoutingKey: func(topic string) string {
			return r.GetRoutingKey()
		},
	}
}

func (r *RabbitMQ) loadPublishConfig() amqp.PublishConfig {
	r.Info("Loading publish config...")
	return amqp.PublishConfig{
		GenerateRoutingKey: func(topic string) string {
			return topic
		},
	}
}

func (r *RabbitMQ) loadConsumeConfig() amqp.ConsumeConfig {
	r.Info("Loading consume config...")
	return amqp.ConsumeConfig{
		Qos: amqp.QosConfig{
			PrefetchCount: 10,
		},
	}
}
