package main

import (
	"ddc_queue/rabbitmq-sample/utils"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/assembla/cony"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"html/template"
	"log"
	"net/http"
)

var port = flag.Int("producer.port", 3000, "producer listening port")

var form = `
	{{ if eq .status "thanks"}}
		<p>Thank you</p>
	{{ end }}
	<form method="post">
		<input type="text" name="body" style="width:300px" />
		<input type="submit" value="Send" />
	</form>
`

func showUsageAndStatusPForm() {
	fmt.Printf("Producer Form is running\n")
	fmt.Printf("Listening on: %v\n\n", *port)
	fmt.Println("Configurations :")
	flag.PrintDefaults()
	fmt.Printf("\n\n")
}

func main() {
	var cfg utils.Config
	utils.ReadConfigFile(&cfg)

	utils.PrepFlagsConfig(cfg)

	showUsageAndStatusPForm()

	// Construct new client with the flag url
	// and default backoff policy
	cli := cony.NewClient(
		cony.URL(cfg.Url),
		cony.Backoff(cony.DefaultBackoff),
	)

	// Declare the exchange we'll be using
	exc := utils.OrderExchange(cfg)
	que := utils.OrderQueue(cfg)
	bnd := utils.OrderBinding(que, exc, cfg)
	cli.Declare([]cony.Declaration{
		cony.DeclareQueue(que),
		cony.DeclareExchange(exc),
		cony.DeclareBinding(bnd),
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

	// Start our loop in a new gorouting
	// so we don't block this one
	go func() {
		for cli.Loop() {
			select {
			case err := <-cli.Errors():
				fmt.Println("cli.Error")
				fmt.Println(err)
			}
		}
	}()

	// Simple template for our web-view
	tpl, err := template.New("formText").Parse(form)
	if err != nil {
		log.Fatal(err)
		return
	}

	// HTTP handler function
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			// "GET" shows the template along
			// with the possible thanks message
			hdr := w.Header()
			hdr["Content-Type"] = []string{"text/html"}
			tpl.Execute(w, map[string]string{
				"status": r.FormValue("status"),
			})
			return
		} else if r.Method == "POST" {
			ipt := map[string]string{
				"body": r.FormValue("body"),
			}
			jsn, err := json.Marshal(ipt)
			if err != nil {
				log.Printf("FATAL Marshal : %s", err.Error())

				_, err = w.Write([]byte(err.Error()))
				if err != nil {
					log.Printf("FATAL w.Write : %s", err.Error())
				}
				return
			}

			log.Printf("PUBLISH : %s", jsn)

			// "POST" publishes the value received
			// from the form to AMQP
			// Note: we're using the "pbl" variable
			// (declared above in our code) and we
			// don't declare a new Publisher value.
			go func() {
				_, err := pbl.Write(jsn)
				if err != nil {
					log.Printf("FATAL Write : %s", err.Error())
				}
			}()
			//go pbl.Publish(amqp.Publishing{
			//	Body: jsn,
			//})
			//go func() {
			//	num := 100
			//	for i := 0; i < num; i++ {
			//		err := pbl.Publish(amqp.Publishing{
			//			Body: jsn,
			//		})
			//
			//		if err != nil {
			//			log.Printf("FATAL : %s", err.Error())
			//		}
			//	}
			//}()

			http.Redirect(w, r, "/?status=thanks", http.StatusFound)
			return
		}
		http.Error(w, "404 not found", http.StatusNotFound)
	})
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", *port), nil))
}
