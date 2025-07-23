package rabbitmq

import (
	  amqp "github.com/rabbitmq/amqp091-go"

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

	return Channel.QueueDeclare(queueName,true, false,false, false,args)



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
