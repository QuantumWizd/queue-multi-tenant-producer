package rabbitmq

import (
	"github.com/streadway/amqp"
)

const (
	QueuePrefix = "idfcBankQueue_"
)

func DeclareTenantQueue(merchantId string) (amqp.Queue, error) {
	queueName := QueuePrefix + merchantId
	args := amqp.Table{
		// Optional TTL to auto-delete inactive queues
		"x-expires": int32(20 * 60 * 1000), // 20 mins TTL after unused
	}

	return Channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused (we control deletion)
		false, // not exclusive
		false, // no-wait
		args,
	)
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
