package rabbitmq

import (
	"log"

	  amqp "github.com/rabbitmq/amqp091-go"

)

var (
	Conn    *amqp.Connection
	Channel *amqp.Channel
)

// InitRabbitMQ initializes a connection to RabbitMQ and sets up a channel.
// It connects to the RabbitMQ server using the default credentials and
// localhost address. If the connection or channel creation fails, the
// function logs the error and terminates the application.
//
// This function is essential for establishing communication with RabbitMQ
// for message queuing and processing.
func InitRabbitMQ() error {
	var err error
	// Establish a connection to RabbitMQ server
	Conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
		return err
	}

	// Create a channel
	Channel, err = Conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
		return err
	}


	log.Println("RabbitMQ connection and channel established successfully")
	return nil
}
