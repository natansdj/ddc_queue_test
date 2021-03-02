package main

import (
	"ddc_queue_test/rabbitmq-sample/utils"
	"encoding/json"
	"fmt"
	"github.com/assembla/cony"
	"log"
)

// Item struct is used to send down a channel
// the map received from a delivery and the callbacks
// for acknowledge(Ack) or negatively acknowledge(Nack)
type item struct {
	ipt  map[string]string
	ack  func(bool) error
	nack func(bool, bool) error
}

func main() {
	var cfg utils.Config
	utils.ReadConfigFile(&cfg)

	utils.PrepFlagsConfig(cfg)

	utils.ShowUsageAndStatusConsumer("Consumer Fanout")

	// Channel used for stopping goroutines
	done := make(chan struct{})

	// Items channel used for sending our deliveries to the workers
	itms := make(chan item)

	// Construct new client with the flag url
	// and default backoff policy
	cli := cony.NewClient(
		cony.URL(cfg.Url),
		cony.Backoff(cony.DefaultBackoff),
	)

	// Declarations
	// The queue name will be supplied by the AMQP server
	que := &cony.Queue{
		Name:       "",
		Durable:    false,
		AutoDelete: true,
		Exclusive:  true,
	}
	exc := cony.Exchange{
		Name:       "logs",
		Kind:       "fanout",
		Durable:    true,
		AutoDelete: true,
	}
	bnd := cony.Binding{
		Queue:    que,
		Exchange: exc,
		Key:      "",
	}
	cli.Declare([]cony.Declaration{
		cony.DeclareQueue(que),
		cony.DeclareExchange(exc),
		cony.DeclareBinding(bnd),
	})

	// Declare and register a consumer
	cns := cony.NewConsumer(
		que,
		//cony.Qos(10),
		//cony.AutoAck(),
		//cony.Tag("OrderConsumer1"),
	)
	cli.Consume(cns)

	// Go routing that uses the cony loop to receive deliveries
	// handle reconnects, etc
	// We use the done channel to exit from this goroutine.

	go func() {
		var i int64
		for cli.Loop() {
			i++
			log.Printf("cli.Loop : %v", i)
			select {
			case msg := <-cns.Deliveries():
				//time.Sleep(500 * time.Millisecond) // naive backoff
				log.Printf("Received body: %q\n", msg.Body)

				//Unmarshal
				var ipt map[string]string
				err := json.Unmarshal(msg.Body, &ipt)
				if err != nil {
					log.Printf("FATAL Unmarshal : %s", err.Error())
					if err = msg.Nack(true, false); err != nil {
						log.Printf("FATAL msg.Nack : %s", err.Error())
					}
					continue //continue loop
				}

				// If when we built the consumer we didn't use
				// the "cony.AutoAck()" option this is where we'd
				// have to call the "amqp.Deliveries" methods "Ack",
				// "Nack", "Reject"
				//
				//msg.Ack(false)
				//msg.Nack(false, false)
				//msg.Reject(false)
				itms <- item{
					ipt:  ipt,
					ack:  msg.Ack,
					nack: msg.Nack,
				}

			case err := <-cns.Errors():
				fmt.Printf("Consumer error: %v\n", err)

			case err := <-cli.Errors():
				fmt.Printf("Client error: %v\n", err)
				cli.Close()
			case <-done:
				return
			}
		}
	}()

	// Workers
	go func() {
		num := 1
		for i := 0; i < num; i++ {
			go func() {
				for {
					select {
					case itm := <-itms:
						err := workerProcess(itm.ipt)
						if err != nil {
							log.Printf("FATAL workerProcess : %s", err.Error())
							if err := itm.nack(false, false); err != nil {
								log.Printf("FATAL itm.nack : %s", err.Error())
							}
						} else {
							if err := itm.ack(true); err != nil {
								log.Printf("FATAL itm.ack : %s", err.Error())
							}
						}
					case <-done:
						log.Println("DONE")
						return
					}
				}
			}()
		}
	}()

	// Block this indefinitely
	<-done
}

func workerProcess(ipt map[string]string) (err error) {
	// mail settings
	log.Printf("workerProcess : %v", ipt)

	//err = errors.New("failed")
	return
}
