package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QueuePrefix = "idfcBankQueue_"
)

func DeclareTenantQueue(context context.Context, merchantId, bankPiplineCode string) (amqp.Queue, error) {

	queueName := bankPiplineCode + QueuePrefix + merchantId

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
