package validator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"analytics/db"
	"analytics/models"
)

const (
	maxRetries       = 2
	retryBackoffBase = time.Second
	minEntityTypeLen = 1
)

func ProcessEventsInDocument(
	db *mongo.Database,
	events []models.Event,
	timeoutSec int,
	maxConcurrent int,
	documentIndex int,
) []models.Result {
	if len(events) == 0 {
		log.Println("Warning: No events to process")
		return nil
	}

	results := make([]models.Result, 0, len(events))
	resultsChan := make(chan models.Result, len(events))
	workerPool := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	for _, event := range events {
		wg.Add(1)
		go func(evt models.Event) {
			defer wg.Done()
			workerPool <- struct{}{}
			defer func() { <-workerPool }()

			result := processEventWithRetry(db, evt, timeoutSec, documentIndex)
			resultsChan <- result
			logEventProcessingResult(result)
		}(event)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	for result := range resultsChan {
		results = append(results, result)
	}

	logProcessingStats(results)
	logResourceUsage(maxConcurrent)
	return results
}

func ProcessEventsWithMySQL(
	mongoDB *mongo.Database,
	mysqlDB *sql.DB,
	events []models.Event,
	timeoutSec int,
	maxConcurrent int,
	csvPath string,
	documentIndex int,
) []models.CombinedResult {
	if len(events) == 0 {
		log.Println("Warning: No events to process")
		return nil
	}

	results := make([]models.CombinedResult, 0, len(events))
	resultsChan := make(chan models.CombinedResult, len(events))
	workerPool := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	for _, event := range events {
		wg.Add(1)
		go func(evt models.Event) {
			defer wg.Done()
			workerPool <- struct{}{}
			defer func() { <-workerPool }()

			mongoResult := processEventWithRetry(mongoDB, evt, timeoutSec, documentIndex)
			combinedResult := db.ValidateAndCheckEvents(evt, mongoResult, mysqlDB, csvPath)
			resultsChan <- combinedResult
		}(event)
	}

	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	for result := range resultsChan {
		results = append(results, result)
	}

	logCombinedProcessingStats(results)
	logResourceUsage(maxConcurrent)
	return results
}

func processEventWithRetry(db *mongo.Database, event models.Event, timeoutSec, documentIndex int) models.Result {
	var result models.Result
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		attemptTimeout := timeoutSec * attempt
		result = validateEvent(db, event, attemptTimeout)
		result.OffsetID = documentIndex

		if result.Error == nil || !isTimeoutError(result.Error) {
			return result
		}

		lastErr = result.Error
		log.Printf("Attempt %d failed for event %s: %v. Retrying...", attempt, event.ID, lastErr)
		time.Sleep(time.Duration(attempt) * retryBackoffBase)
	}

	result.Error = fmt.Errorf("failed after %d attempts, last error: %v", maxRetries, lastErr)
	return result
}

func validateEvent(db *mongo.Database, event models.Event, timeoutSec int) models.Result {
	result := models.Result{
		EventID:        event.ID,
		EntityType:     event.EntityType,
		CollectionName: event.EntityType,
		FoundInDest:    false,
		Event:          event,
	}

	if err := validateEventFields(event); err != nil {
		result.Error = err
		return result
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	collection := db.Collection(event.EntityType)
	filter := bson.M{"event.mappingId": event.ID}
	countOptions := options.Count().SetMaxTime(time.Duration(timeoutSec) * time.Second)
	count, err := collection.CountDocuments(ctx, filter, countOptions)

	if err != nil {
		result.Error = fmt.Errorf("error querying collection: %v", err)
		return result
	}

	result.FoundInDest = count > 0
	return result
}

func validateEventFields(event models.Event) error {
	if len(event.EntityType) < minEntityTypeLen {
		return fmt.Errorf("invalid entity_type: length must be at least %d", minEntityTypeLen)
	}
	if event.ID == "" {
		return fmt.Errorf("event ID cannot be empty")
	}
	return nil
}

func logEventProcessingResult(result models.Result) {
	if result.Error != nil {
		log.Printf("Error checking event %s in collection %s: %v",
			result.EventID, result.CollectionName, result.Error)
	} else if result.FoundInDest {
		// log.Printf("✅ Event %s found in %s collection", result.EventID, result.CollectionName)
	} else {
		log.Printf("❌ Event %s NOT found in %s collection", result.EventID, result.CollectionName)
	}
}

func logProcessingStats(results []models.Result) {
	var found, notFound, errored int
	for _, result := range results {
		if result.Error != nil {
			errored++
		} else if result.FoundInDest {
			found++
		} else {
			notFound++
		}
	}
	log.Printf("Processing Summary: %d events found, %d events not found, %d errors",
		found, notFound, errored)
}

func logCombinedProcessingStats(results []models.CombinedResult) {
	var mongoFound, mongoNotFound, mysqlFound, mysqlNotFound, errors int
	for _, result := range results {
		// Count MongoDB results
		if result.MongoResult.Error != nil {
			errors++
		} else if result.MongoResult.FoundInDest {
			mongoFound++
		} else {
			mongoNotFound++
		}

		// Count MySQL results only if MongoDB validation passed and we have a mapping
		if result.MongoResult.FoundInDest && result.MySQLResult != nil {
			// Skip "no mapping found" as it's not an error
			if result.MySQLResult.Error != nil {
				if !strings.Contains(result.MySQLResult.Error.Error(), "no mapping found") {
					errors++
				}
				continue
			}

			if result.MySQLResult.Found {
				mysqlFound++
			} else {
				mysqlNotFound++
			}
		}
	}

	log.Printf("Combined Processing Summary:\nMongoDB: %d found, %d not found\nMySQL: %d found, %d not found\nErrors: %d",
		mongoFound, mongoNotFound, mysqlFound, mysqlNotFound, errors)
}

func logResourceUsage(maxConcurrent int) {
	log.Printf("Resource Usage:\nMax Concurrent Workers: %d\nCurrent Goroutines: %d",
		maxConcurrent, runtime.NumGoroutine())
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "timeout") ||
		strings.Contains(err.Error(), "deadline exceeded")
}
