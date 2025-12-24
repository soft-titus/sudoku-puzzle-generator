package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type PuzzleMessage struct {
	PuzzleID string `json:"puzzleId"`
}

// generateRandomID generates a random alphanumeric string of length n
func generateRandomID(rng *rand.Rand, n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Optional command-line argument for puzzleId
	var puzzleID string
	var retryCountArg int
	flag.StringVar(&puzzleID, "puzzle-id", "", "Puzzle ID to send")
	flag.IntVar(&retryCountArg, "retry-count", -1, "Retry count to set in header (optional)")
	flag.Parse()

	if puzzleID == "" {
		puzzleID = generateRandomID(rng, 5)
	}

	// Determine retry count
	maxRetriesStr := os.Getenv("TASK_MAX_RETRIES")
	if maxRetriesStr == "" {
		log.Fatal("TASK_MAX_RETRIES environment variable is not set")
	}

	maxRetries, err := strconv.Atoi(maxRetriesStr)
	if err != nil || maxRetries <= 0 {
		log.Fatalf("Invalid TASK_MAX_RETRIES value: %s", maxRetriesStr)
	}

	var retryCount int
	if retryCountArg >= 0 {
		retryCount = retryCountArg
	} else {
		retryCount = rng.Intn(maxRetries)
	}

	kafkaBroker := fmt.Sprintf("%s:%s", os.Getenv("KAFKA_BROKER_HOST"), os.Getenv("KAFKA_BROKER_PORT"))
	puzzleTopic := os.Getenv("KAFKA_PUZZLE_TOPIC")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   puzzleTopic,
	})
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("failed to close kafka writer: %v", err)
		}
	}()

	msg := PuzzleMessage{PuzzleID: puzzleID}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("Failed to marshal message: %v", err)
	}

	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(puzzleID),
			Value: data,
			Headers: []kafka.Header{
				{
					Key:   "retry-count",
					Value: []byte(strconv.Itoa(retryCount)),
				},
			},
		},
	)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	log.Printf("Sent message with puzzleId: %s, retryCount: %d", puzzleID, retryCount)
}
