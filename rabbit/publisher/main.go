package main

import (
	"ddc_queue_test/rabbit/utils"
	utils2 "ddc_queue_test/rabbitmq2/utils"
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"time"
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

	rand.Seed(time.Now().UnixNano())

	addTask := utils.AddTask{Number1: rand.Intn(999), Number2: rand.Intn(999)}
	body, err := json.Marshal(addTask)
	if err != nil {
		utils.HandleError(err, "Error encoding JSON")
	}

	err = amqpChannel.Publish("", cfg.QueueName, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         body,
	})

	if err != nil {
		log.Fatalf("Error publishing message: %s", err)
	}

	log.Printf("AddTask: %d+%d", addTask.Number1, addTask.Number2)

}
