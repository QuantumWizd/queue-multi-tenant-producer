package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/QuantumWizd/queue-multi-tenant-producer/payloads"
	"github.com/QuantumWizd/queue-multi-tenant-producer/rabbitmq"
	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
)

var Channel *amqp.Channel

func SamplePostRequest(context *gin.Context) {

	var request payloads.Sample

	// Bind JSON request
	if err := context.ShouldBindJSON(&request); err != nil {
		context.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request payload", "details": err.Error()})
		return
	}

	log.Printf("Received request: %+v", request)

	// Convert to JSON for RabbitMQ
	jsonBody, err := json.Marshal(request)
	if err != nil {
		context.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process request", "details": err.Error()})
		return
	}
	// Declare queue before publishing
	queue, err := rabbitmq.DeclareTenantQueue(request.UserId)
	if err != nil {
		log.Printf("Failed to declare queue: %v", err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to initialize message queue", "details": err.Error()})
		return
	}

	// Send message to RabbitMQ
	err = Channel.PublishWithContext(context.Request.Context(), "", queue.Name, false, false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonBody,
		},
	)

	if err != nil {
		log.Printf("Failed to publish message: %v", err)
		context.JSON(http.StatusInternalServerError, gin.H{"error":   "Failed to queue request","details": err.Error(),})
		return
	}

	context.JSON(http.StatusAccepted, gin.H{
		"message": "Request processed successfully",
		"data":    request,
	})
}
