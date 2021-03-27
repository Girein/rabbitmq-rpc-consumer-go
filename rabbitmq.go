package rabbitmq

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/Girein/helpers-go"
	"github.com/streadway/amqp"
)

// Connection is the RabbitMQ connection
type Connection struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
	Queue   amqp.Queue
	Err     chan error
}

// Connect initialize the RabbitMQ connection
func (c *Connection) Connect() error {
	var err error

	c.Conn, err = amqp.Dial("amqp://" + os.Getenv("RABBITMQ_USERNAME") + ":" + os.Getenv("RABBITMQ_PASSWORD") + "@" + os.Getenv("RABBITMQ_HOST") + ":" + os.Getenv("RABBITMQ_PORT") + "/" + os.Getenv("RABBITMQ_VHOST"))
	if err != nil {
		helpers.LogIfError(err, "Failed to connect to RabbitMQ")
		return err
	}

	go func() {
		<-c.Conn.NotifyClose(make(chan *amqp.Error))
		c.Err <- errors.New("the connection to RabbitMQ is closed")
	}()

	c.Channel, err = c.Conn.Channel()
	if err != nil {
		helpers.LogIfError(err, "Failed to open a channel in RabbitMQ")
		return err
	}

	c.Queue, err = c.Channel.QueueDeclare(
		os.Getenv("RABBITMQ_QUEUE"), // name
		true,                        // durable
		false,                       // delete when unused
		false,                       // exclusive
		false,                       // no-wait
		nil,                         // arguments
	)
	if err != nil {
		helpers.LogIfError(err, "Failed to declare a queue in RabbitMQ")
		return err
	}

	log.Printf(" [*] Successfully connected to RabbitMQ")

	return nil
}

// Reconnect reconnects the RabbitMQ connection
func (c *Connection) Reconnect(second int) {
	time.Sleep(time.Duration(second) * time.Second)

	log.Printf(" [*] Trying to reconnect to RabbitMQ")

	if err := c.Connect(); err != nil {
		c.Reconnect(second)
	}
}

// Consume consumes the messages from the queues and passes it as map of chan of amqp.Delivery
func (c *Connection) Consume() (map[string]<-chan amqp.Delivery, error) {
	m := make(map[string]<-chan amqp.Delivery)

	err := c.Channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		helpers.LogIfError(err, "Failed to set QoS in RabbitMQ")
		return nil, err
	}

	messages, err := c.Channel.Consume(
		c.Queue.Name, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		helpers.LogIfError(err, "Failed to register a consumer in RabbitMQ")
		return nil, err
	}

	m[c.Queue.Name] = messages

	return m, nil
}

// HandleConsumedMessages handles the consumed messages from the queues
func (c *Connection) HandleConsumedMessages(q string, message <-chan amqp.Delivery, fn func(Connection, string, <-chan amqp.Delivery), second int) {
	for {
		go fn(*c, q, message)

		if err := <-c.Err; err != nil {
			log.Printf(" [*] The connection to RabbitMQ is lost")

			c.Reconnect(second)

			messages, err := c.Consume()
			if err != nil {
				panic(err)
			}

			message = messages[q]

			log.Printf(" [*] Awaiting RPC requests")
		}
	}
}
