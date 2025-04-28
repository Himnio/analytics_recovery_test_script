package db

import (
	"analytics/models"
	"analytics/utils"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Mysql conection config
type MySQLConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// Defult Mysql config returns mysql config
func DefultMySQLConfig() *MySQLConfig {

	user := utils.GetEnvStripping("DB_USER")
	password := utils.GetEnvStripping("DB_PASS")
	host := utils.GetEnvStripping("DB_HOST")
	port := utils.GetEnvStripping("DB_PORT")
	database := utils.GetEnvStripping("DB_NAME")

	log.Println(user, password, host, port, database)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, database)

	return &MySQLConfig{
		DSN:             dsn,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Minute * 3,
	}
}

// ConnectMySQL establishes a connection to MySQL database
func ConnectMySQL(config *MySQLConfig) (*sql.DB, error) {
	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)

	// Test the connection
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	fmt.Println("Connected to MySQL database!")
	return db, nil
}

// EventCollectionMap maps collection names to product_type_id
type EventCollectionMap map[string]int

// LoadEventCollectionMapFromCSV loads collection to product_type_id mapping from a CSV file
func LoadEventCollectionMapFromCSV(filePath string) (EventCollectionMap, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening collection map CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV header: %v", err)
	}

	// Validate header
	if len(header) < 2 || header[0] != "collection_name" || header[1] != "product_type_id" {
		return nil, fmt.Errorf("invalid CSV header format. Expected: collection_name,product_type_id")
	}

	mapping := make(EventCollectionMap)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV record: %v", err)
		}

		// Skip blank rows
		if len(record) < 2 || strings.TrimSpace(record[0]) == "" || strings.TrimSpace(record[1]) == "" {
			continue
		}

		collectionName := strings.TrimSpace(record[0])
		productTypeID, err := strconv.Atoi(strings.TrimSpace(record[1]))
		if err != nil {
			log.Printf("Warning: Skipping row with invalid product_type_id value '%s': %v", record[1], err)
			continue
		}

		mapping[collectionName] = productTypeID
	}

	if len(mapping) == 0 {
		return nil, fmt.Errorf("no valid mappings found in the CSV file")
	}

	return mapping, nil
}

// DefaultEventCollectionMap returns a default mapping of collections to product_type_id
func DefaultEventCollectionMap() EventCollectionMap {
	return EventCollectionMap{
		"other":   0,
		"doctalk": 36,
		// Add more mappings as needed
	}
}

// EventNameMap maps event names to expected screen names
type EventNameMap map[string][]string

// LoadEventNameMapFromCSV loads event name to screen names mapping from a CSV file
func LoadEventNameMapFromCSV(filePath string) (EventNameMap, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening event name map CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV header: %v", err)
	}

	// Validate header
	if len(header) < 2 || header[0] != "event_name" || header[1] != "screen_names" {
		return nil, fmt.Errorf("invalid CSV header format. Expected: event_name,screen_names")
	}

	mapping := make(EventNameMap)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading CSV record: %v", err)
		}

		if len(record) < 2 {
			return nil, fmt.Errorf("invalid CSV record format: %v", record)
		}

		eventName := record[0]
		screenNamesStr := record[1]

		// Split the screen names by comma
		screenNames := strings.Split(screenNamesStr, ",")

		// Trim whitespace from each screen name
		for i, name := range screenNames {
			screenNames[i] = strings.TrimSpace(name)
		}

		// Filter out empty strings
		var filteredScreenNames []string
		for _, name := range screenNames {
			if name != "" {
				filteredScreenNames = append(filteredScreenNames, name)
			}
		}

		mapping[eventName] = filteredScreenNames
	}

	return mapping, nil
}

// DefaultEventNameMap returns a default mapping of event names to screen names
func DefaultEventNameMap() EventNameMap {
	return EventNameMap{
		"DETAIL_EXIT": {"CURRENT_SCREEN", "SERIES_DETAIL"},
		// Add more mappings as needed
	}
}

// CheckEventInMySQL checks if an event exists in MySQL database
func CheckEventInMySQL(db *sql.DB, event models.Event, collectionMap EventCollectionMap, eventNameMap EventNameMap) (*models.MySQLEventResult, error) {
	// Initialize result
	result := &models.MySQLEventResult{
		EventID:        event.ID,
		EventName:      event.EventName,
		CollectionName: event.EntityType,
		SessionID:      event.SessionID,
		Found:          false,
	}

	// Check if we need to verify this event in MySQL
	if len(eventNameMap[event.EventName]) == 0 {
		// No verification needed for this event name
		return result, nil
	}

	// Get product_type_id for the collection
	productTypeID, ok := collectionMap[event.EntityType]
	if !ok {
		return result, fmt.Errorf("no product_type_id mapping for collection: %s", event.EntityType)
	}

	// Construct and execute the query
	query := `
		SELECT id, event_name, product_type, product_type_id, session_id,
		       track_id, session_start_time, session_end_time, date_of_creation
		FROM docquity_analytics.app_tracking_new
		WHERE product_type = ?
		  AND event_name = ?
		  AND session_id = ?
		ORDER BY id DESC LIMIT 1
	`

	row := db.QueryRow(query, productTypeID, event.EventName, event.SessionID)

	// Scan the result into a struct
	var mysqlEvent models.MySQLEvent
	err := row.Scan(
		&mysqlEvent.ID,
		&mysqlEvent.EventName,
		&mysqlEvent.ProductType,
		&mysqlEvent.ProductTypeID,
		&mysqlEvent.SessionID,
		&mysqlEvent.TrackID,
		&mysqlEvent.SessionStartTime,
		&mysqlEvent.SessionEndTime,
		&mysqlEvent.DateOfCreation,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			// No matching record found
			return result, nil
		}
		return result, fmt.Errorf("error querying MySQL: %v", err)
	}

	// Event was found in MySQL
	result.Found = true
	result.MySQLEvent = &mysqlEvent

	return result, nil
}

type EventMapping struct {
	OldEventName      string
	NewEventName      string
	ProductType       int
	CurrentScreenName string
	ActionOn          string
	ActionType        string
	CTAType           string
}

var eventMappings []EventMapping

func LoadEventMappings(csvPath string) error {
	file, err := os.Open(csvPath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %v", err)
	}

	// Verify header format
	expectedHeader := []string{"old_event_name", "new_event_name", "product_type", "current_screen_name", "action_on", "action_type", "cta_type"}
	if !validateHeader(header, expectedHeader[:3]) { // First 3 fields are mandatory
		return fmt.Errorf("invalid CSV header format. Expected mandatory fields: old_event_name, new_event_name, product_type")
	}

	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV records: %v", err)
	}

	eventMappings = make([]EventMapping, 0, len(records))
	for _, record := range records {
		// Skip blank rows (where all fields are empty)
		isBlankRow := true
		for _, field := range record {
			if strings.TrimSpace(field) != "" {
				isBlankRow = false
				break
			}
		}
		if isBlankRow {
			continue
		}

		// Skip if mandatory fields are empty
		if strings.TrimSpace(record[0]) == "" || strings.TrimSpace(record[1]) == "" || strings.TrimSpace(record[2]) == "" {
			log.Printf("Warning: Skipping row with empty mandatory fields: %v", record)
			continue
		}

		productType, err := strconv.Atoi(record[2])
		if err != nil {
			log.Printf("Warning: Invalid product_type in CSV: %s", record[2])
			continue
		}

		mapping := EventMapping{
			OldEventName:      strings.TrimSpace(record[0]),
			NewEventName:      strings.TrimSpace(record[1]),
			ProductType:       productType,
			CurrentScreenName: getOptionalField(record, 3),
			ActionOn:          getOptionalField(record, 4),
			ActionType:        getOptionalField(record, 5),
			CTAType:           getOptionalField(record, 6),
		}
		eventMappings = append(eventMappings, mapping)
	}

	return nil
}

func validateHeader(header []string, mandatoryFields []string) bool {
	if len(header) < len(mandatoryFields) {
		return false
	}
	for i, field := range mandatoryFields {
		if strings.TrimSpace(strings.ToLower(header[i])) != strings.ToLower(field) {
			return false
		}
	}
	return true
}

func getOptionalField(record []string, index int) string {
	if len(record) > index {
		return strings.TrimSpace(record[index])
	}
	return ""
}

func ValidateAndCheckEvents(event models.Event, mongoResult models.Result, mysqlDB *sql.DB, csvPath string) models.CombinedResult {
	if err := LoadEventMappings(csvPath); err != nil {
		log.Printf("Warning: Failed to load event mappings: %v", err)
	}

	result := models.CombinedResult{
		MongoResult: mongoResult,
		MySQLResult: &models.MySQLEventResult{
			EventID:   event.ID,
			EventName: event.EventName,
			SessionID: event.SessionID,
			Found:     false,
		},
	}

	// If MongoDB validation failed, return early
	if !mongoResult.IsSuccess() {
		return result
	}

	// Find matching event mapping
	var mapping *EventMapping
	for _, m := range eventMappings {
		if m.NewEventName == event.EventName {
			mapping = &m
			break
		}
	}

	// If no mapping found, skip MySQL validation
	if mapping == nil {
		result.MySQLResult.Error = fmt.Errorf("no mapping found for event: %s", event.EventName)
		return result
	}

	// Query MySQL for the event using product_type from mapping
	query := `
		SELECT COUNT(*)
		FROM app_tracking_new
		WHERE event_name = ?
		AND session_id = ?
		AND product_type = ?
	`

	var count int
	err := mysqlDB.QueryRow(query, mapping.OldEventName, event.SessionID, mapping.ProductType).Scan(&count)
	if err != nil {
		result.MySQLResult.Error = fmt.Errorf("MySQL query failed: %v", err)
		return result
	}

	result.MySQLResult.Found = count > 0
	if !result.MySQLResult.Found {
		// Get MySQL event details for reporting
		var mysqlEvent models.MySQLEvent
		detailsQuery := `
			SELECT id, event_name, product_type, session_id, track_id
			FROM app_tracking_new
			WHERE session_id = ?
			ORDER BY date_of_creation DESC
			LIMIT 1
		`
		err = mysqlDB.QueryRow(detailsQuery, event.SessionID).Scan(
			&mysqlEvent.ID,
			&mysqlEvent.EventName,
			&mysqlEvent.ProductType,
			&mysqlEvent.SessionID,
			&mysqlEvent.TrackID,
		)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("Warning: Failed to get MySQL event details: %v", err)
		}
		result.MySQLResult.MySQLEvent = &mysqlEvent
	}

	return result
}

// InitializeMappings loads mappings from CSV files with fallback to defaults
func InitializeMappings(collectionMapPath, eventNameMapPath string) (EventCollectionMap, EventNameMap) {
	var collectionMap EventCollectionMap
	var eventNameMap EventNameMap
	var err error

	// Load collection mappings
	collectionMap, err = LoadEventCollectionMapFromCSV(collectionMapPath)
	if err != nil {
		log.Printf("Warning: Failed to load collection mappings from CSV: %v. Using defaults.", err)
		collectionMap = DefaultEventCollectionMap()
	}

	// Load event name mappings
	eventNameMap, err = LoadEventNameMapFromCSV(eventNameMapPath)
	if err != nil {
		log.Printf("Warning: Failed to load event name mappings from CSV: %v. Using defaults.", err)
		eventNameMap = DefaultEventNameMap()
	}

	return collectionMap, eventNameMap
}
