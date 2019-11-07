package main

import (
	"ddc_queue_test/rabbit/mqHandler"
	"ddc_queue_test/rabbit/utils"
	"log"
)

func main() {
	var cfg utils.Config
	utils.ReadConfigFile(&cfg)

	if cfg.RabbitMQ.Url == "" {
		log.Printf("Empty configuration. Please define environment variables in config.yml.")
		return
	}

	amqpURI := cfg.RabbitMQ.Url
	queueName := cfg.RabbitMQ.QueueName
	routingKey := cfg.RabbitMQ.RoutingKey
	exchangeName := cfg.RabbitMQ.ExchangeName
	exchangeType := cfg.RabbitMQ.ExchangeType

	stopChan := make(chan bool)

	queue := mqHandler.NewQueue(amqpURI, queueName, routingKey, exchangeName, exchangeType)
	defer queue.CloseQueue()

	//queue.Consume(func(i string) {
	//	log.Printf("Received message with second consumer: %s", i)
	//})

	queue.Consume(func(i string) {
		log.Printf("Received message with first consumer: %s", i)
	})

	// Stop for program termination
	<-stopChan
}
