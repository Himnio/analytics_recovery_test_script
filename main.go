package main

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"analytics/config"
	"analytics/db"
	"analytics/models"
	"analytics/report"
	"analytics/validator"

	"go.mongodb.org/mongo-driver/mongo"
)

func main() {
	// Define and parse configuration
	cfg := config.ParseFlags()

	// Print configuration
	printConfiguration(cfg)

	// Connect to MongoDB
	client, err := db.ConnectMongoDB(cfg.MongoURI, cfg.ConnectionTimeout)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	// Properly disconnect with context
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		client.Disconnect(ctx)
	}()

	// Get database handle
	database := client.Database(cfg.DatabaseName)

	// Query event_recovery collection
	eventRecoveries, err := db.GetEventRecoveries(database, cfg.CollectionName, cfg.DocLimit, cfg.QueryTimeout)
	if err != nil {
		log.Fatalf("Failed to get event recoveries: %v", err)
	}

	// Process all documents
	results := processAllDocuments(database, eventRecoveries, cfg)

	// Create report for missing data
	report.CreateMissingDataReport(results)
}

func printConfiguration(cfg *config.Configuration) {
	fmt.Printf("Configuration:\n")
	fmt.Printf("  MongoDB URI: %s\n", cfg.MongoURI)
	fmt.Printf("  Database: %s\n", cfg.DatabaseName)
	fmt.Printf("  Collection: %s\n", cfg.CollectionName)
	fmt.Printf("  Query Timeout: %d seconds\n", cfg.QueryTimeout)
	fmt.Printf("  Connection Timeout: %d seconds\n", cfg.ConnectionTimeout)
	fmt.Printf("  Max Concurrent Operations: %d\n", cfg.MaxConcurrent)
	fmt.Println("  GOMAXPROCS:", runtime.GOMAXPROCS(0))
	fmt.Println("  NumCPU:", runtime.NumCPU())

	if cfg.DocLimit > 0 {
		fmt.Printf("  Document Limit: %d\n", cfg.DocLimit)
	} else {
		fmt.Printf("  Document Limit: No limit (processing all documents)\n")
	}
}

func processAllDocuments(db *mongo.Database, eventRecoveries []models.EventRecovery, cfg *config.Configuration) []models.Result {
	// Collect all results from all documents
	var allResults []models.Result

	// Process each document
	for docIndex, recovery := range eventRecoveries {
		fmt.Printf("Processing document %d with %d events\n", docIndex+1, len(recovery.Events))
		results := validator.ProcessEventsInDocument(db, recovery.Events, cfg.QueryTimeout, cfg.MaxConcurrent, docIndex+1)
		allResults = append(allResults, results...)
	}

	return allResults
}
