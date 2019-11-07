package main

import (
	"ddc_queue_test/rabbitmq2/utils"
	"flag"
	"fmt"
	"github.com/assembla/cony"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"time"
)

var body = flag.String("body", "Hello world! 2", "what should be sent")

func showUsageAndStatus2() {
	fmt.Printf("Producer is running\n\n")
	fmt.Println("Flags:")
	flag.PrintDefaults()
	fmt.Printf("\n\n")
	fmt.Println("Publishing:")
	fmt.Printf("Body: %q\n", *body)
	fmt.Printf("\n\n")
}

func main() {
	var cfg utils.Config
	utils.ReadConfigFile(&cfg)

	utils.PrepFlagsConfig(cfg)

	showUsageAndStatus2()

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
		ticker := time.NewTicker(1 * time.Microsecond)

		for {
			i++
			select {
			case <-ticker.C:
				fmt.Printf("Client publishing : %v \n", i)
				err := pbl.Publish(amqp.Publishing{
					Body: []byte(*body),
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
