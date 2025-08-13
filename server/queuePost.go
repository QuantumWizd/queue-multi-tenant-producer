package server

import (
	"log"
	"net/http"

	"github.com/QuantumWizd/queue-multi-tenant-producer/payloads"
	"github.com/QuantumWizd/queue-multi-tenant-producer/rabbitmq"
	"github.com/gin-gonic/gin"
)

func SamplePostRequest(context *gin.Context) {

	var request payloads.Sample

	// Bind JSON request
	if err := context.ShouldBindJSON(&request); err != nil {
		context.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request payload", "details": err.Error()})
		return
	}

	log.Printf("Received request: %+v", request)

	// // Convert to JSON for RabbitMQ
	// jsonBody, err := json.Marshal(request)
	// if err != nil {
	// 	context.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process request", "details": err.Error()})
	// 	return
	// }

	// Declare queue before publishing
	queue, err := rabbitmq.DeclareTenantQueue(context, request.UserId, request.BankPipelineCode)
	if err != nil {
		log.Printf("Failed to declare queue: %v", err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to initialize message queue", "details": err.Error()})
		return
	}

	err = rabbitmq.PublishToTenantQueue(context.Request.Context(), queue.Name, []byte("Hello Queue"))
	if err != nil {
		log.Println("Publish failed:", err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish message"})
		return
	}

	context.JSON(http.StatusAccepted, gin.H{
		"message": "Request processed successfully",
		"data":    request,
	})
}
