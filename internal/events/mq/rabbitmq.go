package mq

import (
	"crypto/tls"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
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
}

type IRabbitMQ interface {
}

func NewRabbitMQ() *RabbitMQ {
	return &RabbitMQ{}
}

func (r *RabbitMQ) LoadConfiguration() {
	r.Info("Loading configuration for RabbitMq...")
	cfg := amqp.Config{
		Connection:      r.loadConnectionConfig(),
		Exchange:        r.loadExchangeConfig(),
		Queue:           r.loadQueueConfig(),
		QueueBind:       r.loadQueueBindConfig(),
		Marshaler:       amqp.DefaultMarshaler{},
		TopologyBuilder: &amqp.DefaultTopologyBuilder{},
		Publish:         r.loadPublishConfig(),
		Consume:         r.loadConsumeConfig(),
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: true,
	}
	cfg.Connection.TLSConfig = tlsCfg

	r.Config = cfg
	r.Info("Configuration Loaded")
}

func (r *RabbitMQ) loadConnectionConfig() amqp.ConnectionConfig {
	return amqp.ConnectionConfig{
		AmqpURI: r.GetUri(),
		AmqpConfig: &amqp091.Config{
			Vhost: r.GetVhost(),
		},
		Reconnect: amqp.DefaultReconnectConfig(),
	}
}

func (r *RabbitMQ) loadExchangeConfig() amqp.ExchangeConfig {
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
	return amqp.QueueBindConfig{
		GenerateRoutingKey: func(topic string) string {
			return r.GetRoutingKey()
		},
	}
}

func (r *RabbitMQ) loadPublishConfig() amqp.PublishConfig {
	return amqp.PublishConfig{
		GenerateRoutingKey: func(topic string) string {
			return topic
		},
	}
}

func (r *RabbitMQ) loadConsumeConfig() amqp.ConsumeConfig {
	return amqp.ConsumeConfig{
		Qos: amqp.QosConfig{
			PrefetchCount: 10,
		},
	}
}
