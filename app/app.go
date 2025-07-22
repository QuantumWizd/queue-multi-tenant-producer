package app

import (
	"github.com/QuantumWizd/queue-multi-tenant-producer/rabbitmq"
	"github.com/gin-gonic/gin"
)

func StartApp() {


	rabbitmq.InitRabbitMQ()

	router := gin.Default()
	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "Application is up and running",
		})
	})
	router.Run()

}
