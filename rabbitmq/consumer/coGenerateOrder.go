package main

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"os"

	"ddc_queue_test/rabbitmq"
	"github.com/rs/zerolog"
	"github.com/streadway/amqp"
)

func main() {
	var cfg rabbitmq.Config
	rabbitmq.ReadConfigFile(&cfg)

	rmq := rabbitmq.New(
		&cfg,
		zerolog.New(os.Stderr).With().Timestamp().Logger(),
	)

	exchange := rabbitmq.Exchange{
		Name:    cfg.ExchangeName,
		Type:    cfg.ExchangeType,
		Durable: true,
	}

	queue := rabbitmq.Queue{
		Name:    cfg.QueueName,
		Durable: true,
	}
	binding := rabbitmq.BindingOptions{
		RoutingKey: cfg.RoutingKey,
	}

	consumerOptions := rabbitmq.ConsumerOptions{
		Tag: "GenerateOrderFeeder",
	}

	consumer, err := rmq.NewConsumer(exchange, queue, binding, consumerOptions)
	if err != nil {
		fmt.Print(err)
		return
	}

	defer consumer.Shutdown()

	err = consumer.QOS(3)
	if err != nil {
		panic(err)
	}

	fmt.Println("GenerateOrderFeeder worker started")
	consumer.RegisterSignalHandler()
	err = consumer.Consume(handler)
	if err != nil {
		log.Error().Msg(err.Error())
	}

}

var handler = func(delivery amqp.Delivery) {
	message := string(delivery.Body)
	fmt.Println(message)
	err := delivery.Ack(false)
	if err != nil {
		log.Error().Msg(err.Error())
	}
}
