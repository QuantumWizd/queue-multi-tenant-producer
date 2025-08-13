package rabbitmq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	redis "github.com/QuantumWizd/queue-multi-tenant-producer"
)

const (
	QueuePrefix = "idfcBankQueue_"
)

func DeclareTenantQueue(context context.Context, merchantId string) (amqp.Queue, error) {

	queueName := QueuePrefix + merchantId

	queue, err := Channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return queue, err
	}

	// Set idle timestamp after declaration
	idleKey := "idle_since:" + queueName
	now := time.Now().Format(time.RFC3339)

	err = redis.Redis.Set(context, idleKey, now, 0).Err()
	if err != nil {
		log.Printf("Failed to set idle timestamp in Redis for %s: %v", queueName, err)
	} else {
		log.Printf("Idle time set for queue %s at %s", queueName, now)
	}

	return queue, nil
}

func DeleteTenantQueue(merchantId string) error {
	queueName := QueuePrefix + merchantId
	_, err := Channel.QueueDelete(
		queueName,
		false, // ifUnused
		false, // ifEmpty
		false, // noWait
	)
	return err
}

func PublishToTenantQueue(ctx context.Context, queueName string, body []byte) error {
	err := Channel.PublishWithContext(
		ctx,
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return err
	}

	return nil
}
