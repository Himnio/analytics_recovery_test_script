package report

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"analytics/db"
	"analytics/models"
)

// CreateMissingDataReport generates a report of missing events
func CreateMissingDataReport(results []models.Result) {
	// Create a report structure
	report := models.MissingDataReport{
		Timestamp:    time.Now().Format(time.RFC3339),
		ByCollection: make(map[string][]models.MissingEvent),
		Errors:       []string{},
	}

	// Count total missing events and gather errors
	totalMissing := 0
	errorsMap := make(map[string]bool) // Use map to deduplicate errors

	// Group missing events by collection
	for _, result := range results {
		// Handle errors
		if result.Error != nil {
			errMsg := fmt.Sprintf("Error checking %s in %s: %v",
				result.EventID, result.CollectionName, result.Error)
			errorsMap[errMsg] = true
			continue
		}

		// Skip if found
		if result.FoundInDest {
			continue
		}

		// Create a missing event entry
		missingEvent := models.MissingEvent{
			ID:         result.Event.ID,
			EntityType: result.Event.EntityType,
			EntityCode: result.Event.EntityCode,
			EventName:  result.Event.EventName,
			UUID:       result.Event.UUID,
			SessionID:  result.Event.SessionID,
			OffsetID:   result.OffsetID,
		}

		// Add to the appropriate collection
		report.ByCollection[result.CollectionName] = append(report.ByCollection[result.CollectionName], missingEvent)
		totalMissing++
	}

	report.TotalCount = totalMissing

	// Convert error map to slice
	for errMsg := range errorsMap {
		report.Errors = append(report.Errors, errMsg)
	}

	// Create report file if there are missing events or errors
	if totalMissing > 0 || len(report.Errors) > 0 {
		writeReportToFile(report, totalMissing)
	} else {
		fmt.Println("No missing events or errors found, no report file created.")
	}
}

// CreateCombinedReport generates a report of missing events from both MongoDB and MySQL
func CreateCombinedReport(results []models.CombinedResult, eventNameMap db.EventNameMap, collectionMap db.EventCollectionMap) {
	// Create a report structure
	report := models.MissingDataReport{
		Timestamp:    time.Now().Format(time.RFC3339),
		ByCollection: make(map[string][]models.MissingEvent),
		MySQLMissing: []models.MySQLMissingEvent{},
		Errors:       []string{},
	}

	// Count total missing events and gather errors
	totalMissing := 0
	mysqlMissing := 0
	errorsMap := make(map[string]bool) // Use map to deduplicate errors

	// Process results
	for _, result := range results {
		// Handle MongoDB errors and missing events
		if result.MongoResult.Error != nil {
			errMsg := fmt.Sprintf("MongoDB Error checking %s in %s: %v",
				result.MongoResult.EventID, result.MongoResult.CollectionName, result.MongoResult.Error)
			errorsMap[errMsg] = true
			continue
		}

		// If not found in MongoDB, add to MongoDB missing list
		if !result.MongoResult.FoundInDest {
			// Create a missing event entry
			missingEvent := models.MissingEvent{
				ID:         result.MongoResult.Event.ID,
				EntityType: result.MongoResult.Event.EntityType,
				EntityCode: result.MongoResult.Event.EntityCode,
				EventName:  result.MongoResult.Event.EventName,
				UUID:       result.MongoResult.Event.UUID,
				SessionID:  result.MongoResult.Event.SessionID,
			}

			// Add to the appropriate collection
			report.ByCollection[result.MongoResult.CollectionName] = append(
				report.ByCollection[result.MongoResult.CollectionName], missingEvent)
			totalMissing++
		}

		// Handle MySQL results if available
		if result.MySQLResult != nil {
			// Handle MySQL errors
			if result.MySQLResult.Error != nil {
				errMsg := fmt.Sprintf("MySQL Error checking %s: %v",
					result.MySQLResult.EventID, result.MySQLResult.Error)
				errorsMap[errMsg] = true
				continue
			}

			// Check if this event should be in MySQL
			shouldBeInMySQL := false
			if _, ok := eventNameMap[result.MySQLResult.EventName]; ok {
				shouldBeInMySQL = true
			}

			// If event should be in MySQL but is not found
			if shouldBeInMySQL && !result.MySQLResult.Found {
				// Get product_type_id for the collection
				productTypeID := 0
				if id, ok := collectionMap[result.MySQLResult.CollectionName]; ok {
					productTypeID = id
				}

				// Create a missing MySQL event entry
				mysqlMissingEvent := models.MySQLMissingEvent{
					ID:           result.MySQLResult.EventID,
					EventName:    result.MySQLResult.EventName,
					SessionID:    result.MySQLResult.SessionID,
					EntityType:   result.MySQLResult.CollectionName,
					ProductType:  productTypeID,
					RequiredInDB: true,
				}

				report.MySQLMissing = append(report.MySQLMissing, mysqlMissingEvent)
				mysqlMissing++
			}
		}
	}

	report.TotalCount = totalMissing
	report.MySQLMissingCount = mysqlMissing

	// Convert error map to slice
	for errMsg := range errorsMap {
		report.Errors = append(report.Errors, errMsg)
	}

	// Create report file if there are missing events or errors
	if totalMissing > 0 || mysqlMissing > 0 || len(report.Errors) > 0 {
		writeCombinedReportToFile(report, totalMissing, mysqlMissing)
	} else {
		fmt.Println("No missing events or errors found, no report file created.")
	}
}

// writeReportToFile writes the report to a JSON file
func writeReportToFile(report models.MissingDataReport, totalMissing int) {
	// Create directory if it doesn't exist
	err := os.MkdirAll("missing_data", 0755)
	if err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}

	// Create a timestamped filename
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("missing_data", fmt.Sprintf("missing_events_%s.json", timestamp))

	// Marshal to JSON with indentation for readability
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	// Write to file
	err = os.WriteFile(filename, jsonData, 0644)
	if err != nil {
		log.Fatalf("Failed to write file: %v", err)
	}

	fmt.Printf("Created missing data report: %s\n", filename)
	fmt.Printf("  - %d missing events\n", totalMissing)
	fmt.Printf("  - %d errors encountered\n", len(report.Errors))
}

// writeCombinedReportToFile writes the combined report to a JSON file
func writeCombinedReportToFile(report models.MissingDataReport, totalMissing, mysqlMissing int) {
	// Create directory if it doesn't exist
	err := os.MkdirAll("missing_data", 0755)
	if err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}

	// Create a timestamped filename
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("missing_data", fmt.Sprintf("combined_missing_events_%s.json", timestamp))

	// Marshal to JSON with indentation for readability
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	// Write to file
	err = os.WriteFile(filename, jsonData, 0644)
	if err != nil {
		log.Fatalf("Failed to write file: %v", err)
	}

	fmt.Printf("Created combined missing data report: %s\n", filename)
	fmt.Printf("  - %d missing MongoDB events\n", totalMissing)
	fmt.Printf("  - %d missing MySQL events\n", mysqlMissing)
	fmt.Printf("  - %d errors encountered\n", len(report.Errors))
}

// CreateEnhancedMissingDataReport creates separate reports for missing events and errors
func CreateEnhancedMissingDataReport(results []models.CombinedResult) {
	// Initialize MongoDB Report
	mongoReport := models.MongoReport{
		Timestamp: time.Now().Format(time.RFC3339),
		Events:    []models.MissingEvent{},
	}

	// Initialize MySQL Report
	mysqlReport := models.MySQLReport{
		Timestamp: time.Now().Format(time.RFC3339),
		Events:    []models.MongoToMySQLMissingEvent{},
	}

	// Initialize Error Report
	errorReport := models.ErrorReport{
		Timestamp: time.Now().Format(time.RFC3339),
		Errors:    []models.ErrorEvent{},
	}

	// Initialize Summary Report
	summaryReport := models.SummaryReport{
		Timestamp:          time.Now().Format(time.RFC3339),
		TotalEventsChecked: len(results),
		MongoMissingCount:  0,
		MySQLMissingCount:  0,
		ErrorCount:         0,
	}

	hasFailures := false

	for _, result := range results {
		// Process MongoDB results first
		if result.MongoResult.Error != nil {
			hasFailures = true
			summaryReport.ErrorCount++

			// Add to error report
			errorEvent := models.ErrorEvent{
				ID:         result.MongoResult.EventID,
				EventName:  result.MongoResult.Event.EventName,
				EntityType: result.MongoResult.EntityType,
				SessionID:  result.MongoResult.Event.SessionID,
				ErrorType:  "mongo",
				ErrorMsg:   result.MongoResult.Error.Error(),
			}
			errorReport.Errors = append(errorReport.Errors, errorEvent)
			continue
		}

		// If event not found in MongoDB, add to MongoDB report
		if !result.MongoResult.FoundInDest {
			hasFailures = true
			summaryReport.MongoMissingCount++
			event := models.MissingEvent{
				ID:         result.MongoResult.EventID,
				EntityType: result.MongoResult.EntityType,
				EventName:  result.MongoResult.Event.EventName,
				UUID:       result.MongoResult.Event.UUID,
				SessionID:  result.MongoResult.Event.SessionID,
				OffsetID:   result.MongoResult.OffsetID,
			}
			mongoReport.Events = append(mongoReport.Events, event)
			continue
		}

		// Only process MySQL results if MongoDB validation passed
		if result.MySQLResult != nil {
			// Skip if no mapping found - this is not an error
			if result.MySQLResult.Error != nil {
				if strings.Contains(result.MySQLResult.Error.Error(), "no mapping found") {
					continue // Skip silently - this is expected for events we don't need to validate
				}
				// Add real errors to error report
				hasFailures = true
				summaryReport.ErrorCount++
				errorEvent := models.ErrorEvent{
					ID:         result.MySQLResult.EventID,
					EventName:  result.MySQLResult.EventName,
					EntityType: result.MongoResult.EntityType,
					SessionID:  result.MySQLResult.SessionID,
					ErrorType:  "mysql",
					ErrorMsg:   result.MySQLResult.Error.Error(),
				}
				errorReport.Errors = append(errorReport.Errors, errorEvent)
				continue
			}

			// If event not found in MySQL, add to MySQL report
			if !result.MySQLResult.Found {
				hasFailures = true
				summaryReport.MySQLMissingCount++

				// Add to MySQL report
				mysqlEvent := models.MongoToMySQLMissingEvent{
					ID:             result.MongoResult.EventID,
					EventName:      result.MongoResult.Event.EventName,
					CollectionName: result.MongoResult.CollectionName,
					SessionID:      result.MongoResult.Event.SessionID,
					OffsetID:       result.MongoResult.OffsetID,
					EntityCode:     result.MongoResult.Event.EntityCode,
					ScreenName:     result.MongoResult.Event.ScreenName,
				}
				mysqlReport.Events = append(mysqlReport.Events, mysqlEvent)
			}
		}
	}

	// Update total counts
	mongoReport.TotalCount = len(mongoReport.Events)
	mysqlReport.TotalCount = len(mysqlReport.Events)
	errorReport.TotalCount = len(errorReport.Errors)

	// Only generate reports if there were failures
	if !hasFailures {
		fmt.Println("All validations passed successfully. No reports generated.")
		return
	}

	// Create missing_data directory if it doesn't exist
	if err := os.MkdirAll("missing_data", 0755); err != nil {
		log.Printf("Error creating missing_data directory: %v", err)
		return
	}

	// Write MongoDB report if there are missing events
	if mongoReport.TotalCount > 0 {
		writeJSONReport("missing_data/mongo_missing_report", mongoReport)
	}

	// Write MySQL report if there are missing events
	if mysqlReport.TotalCount > 0 {
		writeJSONReport("missing_data/mysql_missing_report", mysqlReport)
	}

	// Write error report if there are errors
	if errorReport.TotalCount > 0 {
		writeJSONReport("missing_data/error_report", errorReport)
	}

	// Always write summary report if there are any failures
	writeJSONReport("missing_data/summary_report", summaryReport)
}

func writeJSONReport(prefix string, report interface{}) {
	reportFile := fmt.Sprintf("%s_%s.json", prefix, time.Now().Format("2006-01-02_15-04-05"))
	file, err := os.Create(reportFile)
	if err != nil {
		fmt.Printf("Error creating report file %s: %v\n", reportFile, err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Printf("Error writing report %s: %v\n", reportFile, err)
	}
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
