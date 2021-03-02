package main

import (
	"ddc_queue_test/rabbitmq-sample/utils"
	"flag"
	"fmt"
	"github.com/assembla/cony"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"math/rand"
	"strconv"
	"time"
)

var body *string

func main() {
	rand.Seed(time.Now().UnixNano())
	body = flag.String("body", fmt.Sprintf("Hello world! : %v", rand.Int()), "what should be sent")

	var cfg utils.Config
	utils.ReadConfigFile(&cfg)

	utils.PrepFlagsConfig(cfg)

	utils.ShowUsageAndStatusProducer("Producer Batch", *body)

	// Construct new client with the flag url
	// and default backoff policy
	cli := cony.NewClient(
		cony.URL(cfg.Url),
		cony.Backoff(cony.DefaultBackoff),
	)

	// Declare the exchange we'll be using
	exc := utils.OrderExchange(cfg)
	//que := utils.OrderQueue(cfg)
	//bnd := utils.OrderBinding(que, exc, cfg)
	cli.Declare([]cony.Declaration{
		cony.DeclareExchange(exc),
		//cony.DeclareQueue(que),
		//cony.DeclareBinding(bnd),
	})

	// Declare and register a publisher
	// with the cony client.
	// This needs to be "global" per client
	// and we'll need to use this exact value in
	// our handlers (contexts should be of help)
	msg := amqp.Publishing{
		Headers:         nil,
		ContentType:     "text/plain",
		ContentEncoding: "",
		DeliveryMode:    amqp.Persistent,
		Priority:        1,
		CorrelationId:   "",
		ReplyTo:         "",
		Expiration:      "",
		MessageId:       uuid.New().String(),
		Timestamp:       utils.TimeLocation(),
		Type:            "",
		UserId:          "",
		AppId:           "ddc_queue",
	}
	pbl := cony.NewPublisher(exc.Name, cfg.RoutingKey, cony.PublishingTemplate(msg))
	cli.Publish(pbl)

	// Launch a go routine and publish a message.
	// "Publish" is a blocking method this is why it
	// needs to be called in its own go routine.
	//
	var i int
	go func() {
		ticker := time.NewTicker(1000 * time.Millisecond)

		for {
			i++
			select {
			case <-ticker.C:
				fmt.Printf("Client publishing : %v \n", i)
				bodyMsg := []byte(*body + " - " + strconv.Itoa(i))
				err := pbl.Publish(amqp.Publishing{
					Body: bodyMsg,
				})
				if err != nil {
					fmt.Printf("Client publish error: %v\n", err)
				}
			}
		}
	}()

	// Client loop sends out declarations(exchanges, queues, bindings
	// etc) to the AMQP server. It also handles reconnecting.
	for cli.Loop() {
		select {
		case err := <-cli.Errors():
			fmt.Printf("Client error: %v\n", err)
		case blocked := <-cli.Blocking():
			fmt.Printf("Client is blocked %v\n", blocked)
		}
	}
}
