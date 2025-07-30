package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	redis "github.com/QuantumWizd/queue-multi-tenant-producer"

	rdb "github.com/redis/go-redis/v9"
)

type QueueInfo struct {
	Name      string `json:"name"`
	Messages  int    `json:"messages"`
	Consumers int    `json:"consumers"`
	IdleSince string `json:"idleSince"`
}

const (
	rabbitAPIURL  = "http://localhost:15672/api/queues"
	username      = "guest"
	password      = "guest"
	queuePrefix   = "idfcBankQueue_" // your queue pattern
	idleLimit     = 2 * time.Minute  // adjust as needed
	idleKeyPrefix = "idle_since:"
)

// StartQueueCleanup schedules background cleanup
func StartQueueCleanup() {
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			cleanupIdleQueues(context.Background())
		}

	}()
}

func cleanupIdleQueues(context context.Context) {

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

		idleKey := idleKeyPrefix + q.Name

		if q.Messages > 0 {

			// Reset idle timer if active again
			err := redis.Redis.Set(context, idleKey, now.Format(time.RFC3339), 0)
			if err != nil {
				log.Printf("Failed to clear idle tracker in redis")
			}
			continue
		}

		// Check if this queue already has an idle timestamp
		idleSinceStr, err := redis.Redis.Get(context, idleKey).Result()
		if err == rdb.Nil {
			// Not found → set now as idle start time
			err := redis.Redis.Set(context, idleKey, now.Format(time.RFC3339), 0).Err()
			if err != nil {
				log.Printf("Failed to set idle time for queue %s: %v", q.Name, err)
			} else {
				log.Printf("Tracking idle start for queue: %s", q.Name)
			}
			continue
		} else if err != nil {
			log.Printf("Failed to get idle time from Redis for %s: %v", q.Name, err)
			continue
		}

		// Parse the stored timestamp
		idleSince, err := time.Parse(time.RFC3339, idleSinceStr)
		if err != nil {
			log.Printf("Invalid time format in Redis for %s: %v", q.Name, err)
			continue
		}

		// If idle for more than idleLimit, delete it
		idleFor := now.Sub(idleSince)

		if idleFor > idleLimit {
			log.Printf("Deleting idle queue: %s (idle for %s)", q.Name, idleFor)
			deleteQueue(q.Name)
			err := redis.Redis.Del(context, idleKey).Err()
			if err != nil {
				log.Printf("Failed to delete idle tracker key from Redis for %s: %v", q.Name, err)
			}
		} else {
			log.Printf("Queue %s is still within idle period (%s)", q.Name, idleFor)
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
	log.Println(queues)

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
