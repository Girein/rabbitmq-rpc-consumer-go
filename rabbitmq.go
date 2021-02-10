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
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
	err     chan error
}

// Connect initialize the RabbitMQ connection
func (c *Connection) Connect() error {
	var err error

	c.conn, err = amqp.Dial("amqp://" + os.Getenv("RABBITMQ_USERNAME") + ":" + os.Getenv("RABBITMQ_PASSWORD") + "@" + os.Getenv("RABBITMQ_HOST") + ":" + os.Getenv("RABBITMQ_PORT") + "/" + os.Getenv("RABBITMQ_VHOST"))
	if err != nil {
		helpers.LogIfError(err, "Failed to connect to RabbitMQ")
		return err
	}

	go func() {
		<-c.conn.NotifyClose(make(chan *amqp.Error))
		c.err <- errors.New("The connection to RabbitMQ is closed")
	}()

	c.channel, err = c.conn.Channel()
	if err != nil {
		helpers.LogIfError(err, "Failed to open a channel in RabbitMQ")
		return err
	}

	c.queue, err = c.channel.QueueDeclare(
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

	err := c.channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		helpers.LogIfError(err, "Failed to set QoS in RabbitMQ")
		return nil, err
	}

	messages, err := c.channel.Consume(
		c.queue.Name, // queue
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

	m[c.queue.Name] = messages

	return m, nil
}
