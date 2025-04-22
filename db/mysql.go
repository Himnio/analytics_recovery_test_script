package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"analytics/models"
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
	return &MySQLConfig{
		DSN:             "user:password@tcp(127.0.0.1:3306)/docquity_analytics", //need to update this
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

// ValidateAndCheckEvents validates events in both MongoDB and MySQL
func ValidateAndCheckEvents(
	mongoEvent models.Event,
	mongoResult models.Result,
	mysqlDB *sql.DB,
	collectionMap EventCollectionMap,
	eventNameMap EventNameMap,
) models.CombinedResult {
	combined := models.CombinedResult{
		MongoResult: mongoResult,
		MySQLResult: nil,
	}

	// Only check MySQL if the event matches our criteria
	shouldCheckMySQL := false
	if screenNames, ok := eventNameMap[mongoEvent.EventName]; ok && len(screenNames) > 0 {
		shouldCheckMySQL = true
	}

	if shouldCheckMySQL {
		mysqlResult, err := CheckEventInMySQL(mysqlDB, mongoEvent, collectionMap, eventNameMap)
		if err != nil {
			fmt.Printf("Error checking MySQL for event %s: %v\n", mongoEvent.ID, err)
			mysqlResult = &models.MySQLEventResult{
				EventID:        mongoEvent.ID,
				EventName:      mongoEvent.EventName,
				CollectionName: mongoEvent.EntityType,
				SessionID:      mongoEvent.SessionID,
				Found:          false,
				Error:          err,
			}
		}
		combined.MySQLResult = mysqlResult

		// Log the result
		if mysqlResult.Error != nil {
			fmt.Printf("Error checking MySQL for event %s: %v\n", mysqlResult.EventID, mysqlResult.Error)
		} else if mysqlResult.Found {
			fmt.Printf("✅ Event %s found in MySQL database\n", mysqlResult.EventID)
		} else {
			fmt.Printf("❌ Event %s NOT found in MySQL database\n", mysqlResult.EventID)
		}
	}

	return combined
}
