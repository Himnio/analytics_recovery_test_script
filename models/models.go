package models

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

// Event represents an event in the event array
type Event struct {
	ID         string      `bson:"id" json:"id"`
	EntityType string      `bson:"entity_type" json:"entity_type"`
	EntityCode interface{} `bson:"entity_code" json:"entity_code"`
	EventName  string      `bson:"event_name" json:"event_name"`
	UUID       string      `bson:"uuid" json:"uuid"`
	SessionID  string      `bson:"session_id" json:"session_id"`
}

// EventRecovery represents a document in the new_event_recovery collection
type EventRecovery struct {
	ID     primitive.ObjectID `bson:"_id,omitempty"`
	Events []Event            `bson:"event"`
}

// Result represents the validation result for a single event
type Result struct {
	EventID        string
	EntityType     string
	FoundInDest    bool
	CollectionName string
	Error          error
	Event          Event // Store the entire event for missing data export
	OffsetID       int
}

// MySQLEvent represents a row from the app_tracking_new table
type MySQLEvent struct {
	ID               int64
	EventName        string
	ProductType      int
	ProductTypeID    int
	SessionID        string
	TrackID          string
	SessionStartTime time.Time
	SessionEndTime   time.Time
	DateOfCreation   time.Time
}

// MySQLEventResult represents the result of a MySQL event check
type MySQLEventResult struct {
	EventID        string
	EventName      string
	CollectionName string
	SessionID      string
	Found          bool
	Error          error
	MySQLEvent     *MySQLEvent
}

// CombinedResult combines MongoDB and MySQL results
type CombinedResult struct {
	MongoResult Result
	MySQLResult *MySQLEventResult
}

// MissingDataReport stores information about missing events
type MissingDataReport struct {
	Timestamp         string                    `json:"timestamp"`
	TotalCount        int                       `json:"total_count"`
	ByCollection      map[string][]MissingEvent `json:"by_collection"`
	MySQLMissingCount int                       `json:"mysql_missing_count"`
	MySQLMissing      []MySQLMissingEvent       `json:"mysql_missing_events"`
	Errors            []string                  `json:"errors,omitempty"` // Track errors
}

// MissingEvent stores information about a single missing event
type MissingEvent struct {
	ID         string      `json:"id"`
	EntityType string      `json:"entity_type"`
	EntityCode interface{} `json:"entity_code"`
	EventName  string      `json:"event_name"`
	UUID       string      `json:"uuid"`
	SessionID  string      `json:"session_id"`
	OffsetID   int         `json:"offset_id"`
}

// MySQLMissingEvent stores information about a missing MySQL event
type MySQLMissingEvent struct {
	ID           string `json:"id"`
	EventName    string `json:"event_name"`
	SessionID    string `json:"session_id"`
	EntityType   string `json:"entity_type"`
	ProductType  int    `json:"product_type"`
	RequiredInDB bool   `json:"required_in_db"`
}
