package main

import (
	"ddc_queue_test/rabbit/utils"
	utils2 "ddc_queue_test/rabbitmq2/utils"
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"os"
)

func main() {
	var cfg utils2.Config
	utils2.ReadConfigFile(&cfg)

	conn, err := amqp.Dial(cfg.Url)
	utils.HandleError(err, "Can't connect to AMQP")
	defer conn.Close()

	amqpChannel, err := conn.Channel()
	utils.HandleError(err, "Can't create a amqpChannel")

	defer amqpChannel.Close()

	queue, err := amqpChannel.QueueDeclare(cfg.QueueName, true, false, false, false, nil)
	utils.HandleError(err, "Could not declare `ddc_queue` queue")

	err = amqpChannel.Qos(1, 0, false)
	utils.HandleError(err, "Could not configure QoS")

	messageChannel, err := amqpChannel.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	utils.HandleError(err, "Could not register consumer")

	stopChan := make(chan bool)

	go func() {
		log.Printf("Consumer ready, PID: %d", os.Getpid())
		for d := range messageChannel {
			log.Printf("Received a message: %s", d.Body)

			addTask := &utils.AddTask{}

			err := json.Unmarshal(d.Body, addTask)

			if err != nil {
				log.Printf("Error decoding JSON: %s", err)
			}

			log.Printf("Result of %d + %d is : %d", addTask.Number1, addTask.Number2, addTask.Number1+addTask.Number2)

			if err := d.Ack(false); err != nil {
				log.Printf("Error acknowledging message : %s", err)
			} else {
				log.Printf("Acknowledged message")
			}

		}
	}()

	// Stop for program termination
	<-stopChan

}
