package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	QueuePrefix = "idfcBankQueue_"
)

func DeclareTenantQueue(context context.Context, merchantId string) (amqp.Queue, error) {

	queueName := QueuePrefix + merchantId
	args := amqp.Table{
		// Optional TTL to auto-delete inactive queues
		"x-expires": int32(20 * 1000), // 20 secs TTL after unused
	}

	return Channel.QueueDeclare(queueName, true, false, false, false, args)

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
	return Channel.PublishWithContext(
		ctx,
		"",
		queueName,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}

