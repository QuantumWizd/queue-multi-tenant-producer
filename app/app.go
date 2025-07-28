package app

import (
	"log"

	"github.com/QuantumWizd/queue-multi-tenant-producer/rabbitmq"
	"github.com/QuantumWizd/queue-multi-tenant-producer/server"
	"github.com/gin-gonic/gin"
)

func StartApp() {

	rabbitmq.InitRabbitMQ()
	server.StartQueueCleanup()

	router := gin.Default()
	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "Application is up and running",
		})
	})
	router.POST("/sample-request", server.SamplePostRequest)

	// Start the server (this blocks)
	port := ":8080" // or your preferred port
	log.Printf("Server starting on port %s", port)
	if err := router.Run(port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
