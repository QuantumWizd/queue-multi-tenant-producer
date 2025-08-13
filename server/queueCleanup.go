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

	for _, q := range queues {

		currentTime := time.Now()
		log.Println(currentTime)

		if !strings.HasPrefix(q.Name, queuePrefix) {
			log.Println(q.Name)
			continue
		}

		if q.Messages <= 0 {
			idleKey := idleKeyPrefix + q.Name

			queueTime, err := redis.Redis.Get(context, idleKey).Result()

			if err == rdb.Nil {

				err := redis.Redis.Set(context, idleKey, currentTime.Format(time.RFC3339), 0).Err()
				if err != nil {
					log.Printf("Failed to set idle time for %s: %v", q.Name, err)
				} else {
					log.Printf("Set idle timestamp for queue: %s", q.Name)
				}
				continue
			} else if err != nil {
				log.Printf("Failed to get idle time from Redis for %s: %v", q.Name, err)
				continue
			}

			idleSince, err := time.Parse(time.RFC3339, queueTime)
			if err != nil {
				log.Printf("Invalid time format in Redis for %s: %v", q.Name, err)
				continue
			}

			if currentTime.Sub(idleSince) > idleLimit {
				log.Printf("Deleting idle queue: %s (idle for %s)", q.Name, currentTime.Sub(idleSince))
				deleteQueue(q.Name)

				err := redis.Redis.Del(context, idleKey).Err()
				if err != nil {
					log.Printf("Failed to delete idle tracker key from Redis for %s: %v", q.Name, err)
				}
			} else {
				log.Printf("Queue %s is still within idle period (%s)", q.Name, currentTime.Sub(idleSince))
			}
		} else {
			idleKey := idleKeyPrefix + q.Name
			err := redis.Redis.Del(context, idleKey).Err()
			if err != nil && err != rdb.Nil {
				log.Printf("Failed to reset idle time for %s: %v", q.Name, err)
			}
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
