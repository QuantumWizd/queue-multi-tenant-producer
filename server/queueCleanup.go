package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

type QueueInfo struct {
	Name      string `json:"name"`
	Messages  int    `json:"messages"`
	Consumers int    `json:"consumers"`
	IdleSince string `json:"idleSince"`
}

const (
	rabbitAPIURL = "http://localhost:15672/api/queues"
	username     = "guest"
	password     = "guest"
	queuePrefix  = "idfcBankQueue_" // your queue pattern
	idleLimit    = 20 * time.Minute // adjust as needed
)

// StartQueueCleanup schedules background cleanup
func StartQueueCleanup() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			cleanupIdleQueues()
		}
	}()
}

func cleanupIdleQueues() {

	queues, err := getAllQueues()
	if err != nil {
		log.Printf("Failed to fetch queues: %v", err)
		return
	}

	now := time.Now()

	for _, q := range queues {
		if !strings.HasPrefix(q.Name, queuePrefix) {
			continue
		}

		if q.Messages > 0 || q.Consumers > 0 {
			continue
		}

		if q.IdleSince == "" {
			log.Printf("Queue %s has no idle_since, skipping\n", q.Name)
			continue
		}

		parsed, err := time.Parse("2006-01-02 15:04:05", q.IdleSince)
		if err != nil {
			log.Printf("Invalid idle_since time for %s: %v\n", q.Name, err)
			continue
		}

		if now.Sub(parsed) > idleLimit {
			log.Printf("Deleting idle queue: %s\n", q.Name)
			deleteQueue(q.Name)
		}
	}
}

func getAllQueues() ([]QueueInfo, error) {

	req, _ := http.NewRequest("GET", rabbitAPIURL, nil)
	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var queues []QueueInfo
	if err := json.NewDecoder(resp.Body).Decode(&queues); err != nil {
		return nil, err
	}

	return queues, nil
}

func deleteQueue(queueName string) {
	
	url := fmt.Sprintf("%s/%%2F/%s", rabbitAPIURL, queueName)

	req, _ := http.NewRequest("DELETE", url, nil)
	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Error deleting %s: %v\n", queueName, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		log.Printf("Deleted queue: %s\n", queueName)
	} else {
		log.Printf("Failed to delete %s — status: %d\n", queueName, resp.StatusCode)
	}
}
