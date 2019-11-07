package mqHandler

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

type RabbitQueue struct {
	url          string
	name         string
	routingKey   string
	exchangeName string
	exchangeType string

	done           chan bool
	errorChannel   chan *amqp.Error
	confirmChannel chan amqp.Confirmation
	connection     *amqp.Connection
	channel        *amqp.Channel
	closed         bool
	logger         *log.Logger

	consumers []messageConsumer
}

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var (
	errNotConnected  = errors.New("not connected to the queue")
	errNotConfirmed  = errors.New("message not confirmed")
	errAlreadyClosed = errors.New("already closed: not connected to the queue")
)

type messageConsumer func(string)

func NewQueue(url, qName, routingKey, excName, excType string) *RabbitQueue {
	q := new(RabbitQueue)
	q.url = url
	q.name = qName
	q.routingKey = routingKey
	q.exchangeName = excName
	q.exchangeType = excType
	q.consumers = make([]messageConsumer, 0)
	q.done = make(chan bool)
	q.logger = log.New(os.Stdout, "", log.LstdFlags)

	q.connect()
	go q.reconnector()

	return q
}

func (q *RabbitQueue) Send(message string) error {
	if q.closed {
		return errors.New("failed to push push: not connected")
	}
	for {
		err := q.UnsafePush(message)
		if err != nil {
			q.logger.Println("Push failed. Retrying...")
			continue
		}
		select {
		case confirm := <-q.confirmChannel:
			if confirm.Ack {
				q.logger.Println("Push confirmed!")
				return nil
			}
		case <-time.After(resendDelay):
		}
		q.logger.Println("Push didn't confirm. Retrying...")
	}
}

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will receive the message.
func (q *RabbitQueue) UnsafePush(message string) (err error) {
	if q.closed {
		return errNotConnected
	}
	err = q.channel.Publish(
		"",     // exchange
		q.name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		})
	q.logError("Sending message to queue failed", err)

	return err
}

func (q *RabbitQueue) Consume(consumer messageConsumer) {
	q.logger.Println("Registering consumer...")
	deliveries, err := q.registerQueueConsumer()

	q.logger.Println("Consumer registered! Processing messages...")
	q.executeMessageConsumer(err, consumer, deliveries, false)
}

func (q *RabbitQueue) CloseQueue() (err error) {
	q.logger.Println("Closing connection")
	q.closed = true

	err = q.channel.Close()
	if err != nil {
		q.logError("Channel close error.", err)
		return err
	}

	err = q.connection.Close()
	if err != nil {
		q.logError("Connection close error.", err)
		return err
	}

	close(q.done)
	return
}

func (q *RabbitQueue) reconnector() {
	for {
		err := <-q.errorChannel

		if <-q.done {
			return
		}

		if !q.closed {
			q.logError("Reconnecting after connection closed", err)

			q.connect()
			q.recoverConsumers()
		}
	}
}

func (q *RabbitQueue) connect() {
	for {
		q.logger.Printf("Connecting to rabbitmq on %s\n", q.url)
		conn, err := amqp.Dial(q.url)
		if err == nil {
			q.connection = conn

			q.openChannel()
			q.logger.Println("Connection established!")

			_ = q.declareExchange()
			_ = q.declareQueue()
			_ = q.queueBind()

			q.errorChannel = make(chan *amqp.Error)
			q.confirmChannel = make(chan amqp.Confirmation)
			q.channel.NotifyClose(q.errorChannel)
			q.channel.NotifyPublish(q.confirmChannel)

			return
		}

		q.logError(fmt.Sprintf("Connection to rabbitmq failed. Retrying in %s sec... ", reconnectDelay), err)
		time.Sleep(reconnectDelay)
	}
}

func (q *RabbitQueue) declareQueue() (err error) {
	_, err = q.channel.QueueDeclare(
		q.name, // name
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	q.logError("Queue declaration failed", err)

	if err == nil {
		err = q.channel.Qos(1, 0, false)
		q.logError("Could not configure QoS", err)
	}

	return
}

func (q *RabbitQueue) declareExchange() (err error) {
	err = q.channel.ExchangeDeclare(
		q.exchangeName, // name of the exchange
		q.exchangeType, // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	)
	q.logError("Exchange declaration failed", err)

	return
}

func (q *RabbitQueue) queueBind() (err error) {
	//binding to exchange then this queue can receive message from exchange with routing key
	err = q.channel.QueueBind(
		q.name,         // name of the queue
		q.routingKey,   // routing key
		q.exchangeName, // exchange name
		false,          // noWait
		nil,            // arguments
	)
	q.logError("Queue binding failed", err)

	return
}

func (q *RabbitQueue) openChannel() {
	channel, err := q.connection.Channel()
	q.logError("Opening channel failed", err)
	q.channel = channel

	_ = q.channel.Confirm(false)
}

func (q *RabbitQueue) registerQueueConsumer() (<-chan amqp.Delivery, error) {
	msgs, err := q.channel.Consume(
		q.name, // RabbitQueue
		"",     // messageConsumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	q.logError("Consuming messages from queue failed", err)
	return msgs, err
}

func (q *RabbitQueue) executeMessageConsumer(err error, consumer messageConsumer, deliveries <-chan amqp.Delivery, isRecovery bool) {
	if err == nil {
		if !isRecovery {
			q.consumers = append(q.consumers, consumer)
		}
		go func() {
			for delivery := range deliveries {
				consumer(string(delivery.Body[:]))
			}
		}()
	}
}

func (q *RabbitQueue) recoverConsumers() {
	for i := range q.consumers {
		var consumer = q.consumers[i]

		q.logger.Println("Recovering consumer...")
		msgs, err := q.registerQueueConsumer()
		q.logger.Println("Consumer recovered! Continuing message processing...")
		q.executeMessageConsumer(err, consumer, msgs, true)
	}
}

func (q *RabbitQueue) logError(message string, err error) {
	if err != nil {
		q.logger.Printf("%s: %s", message, err)
	}
}
