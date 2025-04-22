package db

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"analytics/models"
)

// ConnectMongoDB establishes a connection to MongoDB
func ConnectMongoDB(mongoURI string, timeoutSec int) (*mongo.Client, error) {
	// Set client options
	clientOptions := options.Client().
		ApplyURI(mongoURI).
		SetConnectTimeout(time.Duration(timeoutSec) * time.Second).
		SetServerSelectionTimeout(time.Duration(timeoutSec) * time.Second).
		SetSocketTimeout(time.Duration(timeoutSec) * time.Second).
		SetMaxPoolSize(100).            // Adjust connection pool settings
		SetMinPoolSize(10).             // Set minimum connections to avoid slow startup
		SetMaxConnIdleTime(time.Minute) // Close idle connections after 1 minute

	// Connect to MongoDB
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	// Check the connection with a ping
	pingCtx, pingCancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer pingCancel()
	err = client.Ping(pingCtx, readpref.Primary())
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	fmt.Println("Connected to MongoDB!")
	return client, nil
}

// GetEventRecoveries retrieves EventRecovery documents from MongoDB
func GetEventRecoveries(db *mongo.Database, collectionName string, limit int, timeoutSec int) ([]models.EventRecovery, error) {
	var eventRecoveries []models.EventRecovery
	collection := db.Collection(collectionName)

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	// Set up options
	findOptions := options.Find().
		SetNoCursorTimeout(true). // Prevent cursor timeouts
		SetBatchSize(100)         // Smaller batches for better performance

	if limit > 0 {
		findOptions.SetLimit(int64(limit))
	}

	// Find documents
	cursor, err := collection.Find(ctx, bson.M{}, findOptions)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Iterate through the cursor and decode each document
	for cursor.Next(ctx) {
		var eventRecovery models.EventRecovery
		if err := cursor.Decode(&eventRecovery); err != nil {
			return nil, err
		}
		eventRecoveries = append(eventRecoveries, eventRecovery)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	fmt.Printf("Found %d documents in %s collection\n", len(eventRecoveries), collectionName)
	if limit > 0 && len(eventRecoveries) < limit {
		fmt.Printf("Note: Requested %d documents but only found %d\n", limit, len(eventRecoveries))
	}

	return eventRecoveries, nil
}
