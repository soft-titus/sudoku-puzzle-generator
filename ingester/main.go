package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	// Command-line arguments
	puzzleId := flag.String("puzzleId", "", "Puzzle ID (required)")
	puzzleSize := flag.Int("puzzleSize", 9, "Puzzle size (default 9)")
	level := flag.String("level", "EASY", "Puzzle level (default EASY)")
	status := flag.String("status", "GENERATING_PUZZLE", "Puzzle status (default GENERATING_PUZZLE)")
	flag.Parse()

	if *puzzleId == "" {
		log.Fatal("puzzleId is required")
	}

	// MongoDB connection details from environment variables
	host := os.Getenv("MONGO_HOST")
	port := os.Getenv("MONGO_PORT")
	user := os.Getenv("MONGO_USER")
	password := os.Getenv("MONGO_PASSWORD")
	dbName := os.Getenv("MONGO_DB")
	collectionName := os.Getenv("MONGO_COLLECTION_NAME")
	optionsStr := os.Getenv("MONGO_OPTIONS")

	uri := fmt.Sprintf("mongodb://%s:%s@%s:%s/%s?%s", user, password, host, port, dbName, optionsStr)
	clientOpts := options.Client().ApplyURI(uri)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer func() {
		if err := client.Disconnect(context.Background()); err != nil {
			log.Printf("failed to disconnect mongo client: %v", err)
		}
	}()

	collection := client.Database(dbName).Collection(collectionName)

	now := time.Now()

	// Upsert: update if exists, insert if not
	filter := bson.M{"puzzleId": *puzzleId}
	update := bson.M{
		"$set": bson.M{
			"puzzleSize": *puzzleSize,
			"level":      *level,
			"status":     *status,
			"updatedAt":  now,
		},
		"$setOnInsert": bson.M{
			"createdAt": now,
		},
	}
	opts := options.Update().SetUpsert(true)

	res, err := collection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Fatalf("Failed to upsert document: %v", err)
	}

	if res.MatchedCount > 0 {
		fmt.Printf("Updated existing puzzle with ID %s\n", *puzzleId)
	} else if res.UpsertedCount > 0 {
		fmt.Printf("Inserted new puzzle with ID %s\n", *puzzleId)
	} else {
		fmt.Printf("No changes made to puzzle with ID %s\n", *puzzleId)
	}
}
