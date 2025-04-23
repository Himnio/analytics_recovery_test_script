package report

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

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
