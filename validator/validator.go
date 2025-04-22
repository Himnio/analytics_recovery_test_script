package validator

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"analytics/models"
)

// ProcessEventsInDocument processes all events in a document with concurrency control
func ProcessEventsInDocument(db *mongo.Database, events []models.Event, timeoutSec int, maxConcurrent int, documentIndex int) []models.Result {
	var results []models.Result
	var resultsMutex sync.Mutex // To safely append to results from multiple goroutines

	// Create a semaphore to limit concurrency
	semaphore := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	// Process each event
	for _, event := range events {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire a spot in the semaphore

		go func(evt models.Event) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release the semaphore spot when done

			// Process the event with retry logic
			var result models.Result

			// Try up to 2 times with increasing timeout
			for attempt := 1; attempt <= 2; attempt++ {
				// Use longer timeout for retries
				attemptTimeout := timeoutSec * attempt
				result = validateEvent(db, evt, attemptTimeout)

				//set the document index in the result
				result.OffsetID = documentIndex

				// If successful or error is not timeout related, break
				if result.Error == nil || !isTimeoutError(result.Error) {
					break
				}

				fmt.Printf("Attempt %d failed for event %s: %v. Retrying...\n",
					attempt, evt.ID, result.Error)
				time.Sleep(time.Duration(attempt) * time.Second) // Backoff
			}

			// Add result to our results slice thread-safely
			resultsMutex.Lock()
			results = append(results, result)
			resultsMutex.Unlock()

			// Log the outcome
			if result.Error != nil {
				fmt.Printf("Error checking event %s in collection %s: %v\n",
					result.EventID, result.CollectionName, result.Error)
			} else if result.FoundInDest {
				fmt.Printf("✅ Event %s found in %s collection\n", result.EventID, result.CollectionName)
			} else {
				fmt.Printf("❌ Event %s NOT found in %s collection\n", result.EventID, result.CollectionName)
			}
		}(event)
	}

	fmt.Printf("Peak goroutines during processing: %d\n", runtime.NumGoroutine())

	// Wait for all events to be processed
	wg.Wait()

	// Count results
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

	fmt.Printf("Summary: %d events found, %d events not found, %d errors\n", found, notFound, errored)
	return results
}

// validateEvent checks if an event exists in its destination collection
func validateEvent(db *mongo.Database, event models.Event, timeoutSec int) models.Result {
	// Initialize result
	result := models.Result{
		EventID:        event.ID,
		EntityType:     event.EntityType,
		CollectionName: event.EntityType, // Using entity_type as collection name
		FoundInDest:    false,
		Event:          event, // Store the entire event for missing data export
	}

	// Skip if entity_type is empty or invalid
	if event.EntityType == "" {
		result.Error = fmt.Errorf("empty entity_type")
		return result
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	// Get the destination collection
	collection := db.Collection(event.EntityType)

	// Query the collection for the event ID
	filter := bson.M{"event.mappingId": event.ID}

	// Use CountDocuments with timeout options
	countOptions := options.Count().
		SetMaxTime(time.Duration(timeoutSec) * time.Second)

	count, err := collection.CountDocuments(ctx, filter, countOptions)
	if err != nil {
		result.Error = err
		return result
	}

	// If count > 0, the document exists
	result.FoundInDest = count > 0
	return result
}

// Check if an error is timeout related
func isTimeoutError(err error) bool {
	errMsg := err.Error()
	return (errMsg != "" &&
		(contains(errMsg, "deadline exceeded") ||
			contains(errMsg, "timed out") ||
			contains(errMsg, "timeout")))
}

// Simple string contains helper
func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:(len(s)-len(substr)+1)] != substr
}

// package validator
//
// import (
// 	"context"
// 	"database/sql"
// 	"fmt"
// 	"runtime"
// 	"sync"
// 	"time"
//
// 	"go.mongodb.org/mongo-driver/bson"
// 	"go.mongodb.org/mongo-driver/mongo"
// 	"go.mongodb.org/mongo-driver/mongo/options"
//
// 	"analytics/db"
// 	"analytics/models"
// )
//
// // ProcessEventsInDocument processes all events in a document with concurrency control
// func ProcessEventsInDocument(db *mongo.Database, events []models.Event, timeoutSec int, maxConcurrent int) []models.Result {
// 	var results []models.Result
// 	var resultsMutex sync.Mutex // To safely append to results from multiple goroutines
//
// 	// Create a semaphore to limit concurrency
// 	semaphore := make(chan struct{}, maxConcurrent)
// 	var wg sync.WaitGroup
//
// 	// Process each event
// 	for _, event := range events {
// 		wg.Add(1)
// 		semaphore <- struct{}{} // Acquire a spot in the semaphore
//
// 		go func(evt models.Event) {
// 			defer wg.Done()
// 			defer func() { <-semaphore }() // Release the semaphore spot when done
//
// 			// Process the event with retry logic
// 			var result models.Result
//
// 			// Try up to 2 times with increasing timeout
// 			for attempt := 1; attempt <= 2; attempt++ {
// 				// Use longer timeout for retries
// 				attemptTimeout := timeoutSec * attempt
// 				result = validateEvent(db, evt, attemptTimeout)
//
// 				// If successful or error is not timeout related, break
// 				if result.Error == nil || !isTimeoutError(result.Error) {
// 					break
// 				}
//
// 				fmt.Printf("Attempt %d failed for event %s: %v. Retrying...\n",
// 					attempt, evt.ID, result.Error)
// 				time.Sleep(time.Duration(attempt) * time.Second) // Backoff
// 			}
//
// 			// Add result to our results slice thread-safely
// 			resultsMutex.Lock()
// 			results = append(results, result)
// 			resultsMutex.Unlock()
//
// 			// Log the outcome
// 			if result.Error != nil {
// 				fmt.Printf("Error checking event %s in collection %s: %v\n",
// 					result.EventID, result.CollectionName, result.Error)
// 			} else if result.FoundInDest {
// 				fmt.Printf("✅ Event %s found in %s collection\n", result.EventID, result.CollectionName)
// 			} else {
// 				fmt.Printf("❌ Event %s NOT found in %s collection\n", result.EventID, result.CollectionName)
// 			}
// 		}(event)
// 	}
//
// 	fmt.Printf("Peak goroutines during processing: %d\n", runtime.NumGoroutine())
//
// 	// Wait for all events to be processed
// 	wg.Wait()
//
// 	// Count results
// 	var found, notFound, errored int
// 	for _, result := range results {
// 		if result.Error != nil {
// 			errored++
// 		} else if result.FoundInDest {
// 			found++
// 		} else {
// 			notFound++
// 		}
// 	}
//
// 	fmt.Printf("Summary: %d events found, %d events not found, %d errors\n", found, notFound, errored)
// 	return results
// }
//
// // ProcessEventsWithMySQL processes events in both MongoDB and MySQL
// func ProcessEventsWithMySQL(
// 	mongoDB *mongo.Database,
// 	mysqlDB *sql.DB,
// 	events []models.Event,
// 	timeoutSec int,
// 	maxConcurrent int,
// 	collectionMap db.EventCollectionMap,
// 	eventNameMap db.EventNameMap,
// ) []models.CombinedResult {
// 	var combinedResults []models.CombinedResult
// 	var resultsMutex sync.Mutex // To safely append to results from multiple goroutines
//
// 	// Create a semaphore to limit concurrency
// 	semaphore := make(chan struct{}, maxConcurrent)
// 	var wg sync.WaitGroup
//
// 	// Process each event
// 	for _, event := range events {
// 		wg.Add(1)
// 		semaphore <- struct{}{} // Acquire a spot in the semaphore
//
// 		go func(evt models.Event) {
// 			defer wg.Done()
// 			defer func() { <-semaphore }() // Release the semaphore spot when done
//
// 			// First validate in MongoDB
// 			mongoResult := validateEvent(mongoDB, evt, timeoutSec)
//
// 			// Then check in MySQL if needed
// 			combinedResult := db.ValidateAndCheckEvents(evt, mongoResult, mysqlDB, collectionMap, eventNameMap)
//
// 			// Add result to our results slice thread-safely
// 			resultsMutex.Lock()
// 			combinedResults = append(combinedResults, combinedResult)
// 			resultsMutex.Unlock()
// 		}(event)
// 	}
//
// 	// Wait for all events to be processed
// 	wg.Wait()
//
// 	// Count results
// 	var mongoFound, mongoNotFound, mysqlFound, mysqlNotFound, errored int
// 	for _, result := range combinedResults {
// 		if result.MongoResult.Error != nil {
// 			errored++
// 			continue
// 		}
//
// 		if result.MongoResult.FoundInDest {
// 			mongoFound++
// 		} else {
// 			mongoNotFound++
// 		}
//
// 		if result.MySQLResult != nil {
// 			if result.MySQLResult.Error != nil {
// 				errored++
// 			} else if result.MySQLResult.Found {
// 				mysqlFound++
// 			} else {
// 				mysqlNotFound++
// 			}
// 		}
// 	}
//
// 	fmt.Printf("Summary: MongoDB: %d found, %d not found; MySQL: %d found, %d not found; %d errors\n",
// 		mongoFound, mongoNotFound, mysqlFound, mysqlNotFound, errored)
// 	return combinedResults
// }
//
// // validateEvent checks if an event exists in its destination collection
// func validateEvent(db *mongo.Database, event models.Event, timeoutSec int) models.Result {
// 	// Initialize result
// 	result := models.Result{
// 		EventID:        event.ID,
// 		EntityType:     event.EntityType,
// 		CollectionName: event.EntityType, // Using entity_type as collection name
// 		FoundInDest:    false,
// 		Event:          event, // Store the entire event for missing data export
// 	}
//
// 	// Skip if entity_type is empty or invalid
// 	if event.EntityType == "" {
// 		result.Error = fmt.Errorf("empty entity_type")
// 		return result
// 	}
//
// 	// Create a context with timeout
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
// 	defer cancel()
//
// 	// Get the destination collection
// 	collection := db.Collection(event.EntityType)
//
// 	// Query the collection for the event ID
// 	filter := bson.M{"event.mappingId": event.ID}
//
// 	// Use CountDocuments with timeout options
// 	countOptions := options.Count().
// 		SetMaxTime(time.Duration(timeoutSec) * time.Second)
//
// 	count, err := collection.CountDocuments(ctx, filter, countOptions)
// 	if err != nil {
// 		result.Error = err
// 		return result
// 	}
//
// 	// If count > 0, the document exists
// 	result.FoundInDest = count > 0
// 	return result
// }
//
// // Check if an error is timeout related
// func isTimeoutError(err error) bool {
// 	errMsg := err.Error()
// 	return (errMsg != "" &&
// 		(contains(errMsg, "deadline exceeded") ||
// 			contains(errMsg, "timed out") ||
// 			contains(errMsg, "timeout")))
// }
//
// // Simple string contains helper
// func contains(s, substr string) bool {
// 	return len(s) >= len(substr) && s[:(len(s)-len(substr)+1)] != substr
// }
