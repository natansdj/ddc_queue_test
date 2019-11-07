package main

import (
	"fmt"
	"github.com/rs/zerolog"
	"os"

	"ddc_queue_test/rabbitmq"
	"github.com/streadway/amqp"
)

func main() {
	var cfg rabbitmq.Config
	rabbitmq.ReadConfigFile(&cfg)

	rmq := rabbitmq.New(
		&cfg,
		zerolog.New(os.Stderr).With().Timestamp().Logger(),
	)

	exchange := rabbitmq.Exchange{}

	queue := rabbitmq.Queue{
		Name: cfg.QueueName,
	}
	publishingOptions := rabbitmq.PublishingOptions{
		//Tag:        "OrderGeneratorTag",
		RoutingKey: cfg.RoutingKey,
	}

	publisher, err := rmq.NewProducer(exchange, queue, publishingOptions)
	if err != nil {
		panic(err)
	}
	defer publisher.Shutdown()
	publisher.RegisterSignalHandler()

	// may be we should autoconvert to byte array?
	msg := amqp.Publishing{
		Body: []byte("2"),
	}

	publisher.NotifyReturn(func(message amqp.Return) {
		fmt.Println(message)
	})

	for i := 0; i < 10; i++ {
		fmt.Println(i, msg)
		err = publisher.Publish(msg)
		if err != nil {
			fmt.Println(err, i)
		}
	}
}
