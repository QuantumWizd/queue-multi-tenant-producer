package server

import (
	"log"
	"net/http"

	"github.com/QuantumWizd/queue-multi-tenant-producer/payloads"
	"github.com/gin-gonic/gin"
)


func SamplePostRequest(context *gin.Context){

	var request payloads.Sample

	log.Println(request)

	// parse string(request)
	//send message to queue

	context.JSON(http.StatusAccepted,"Request processed successfully")

}

