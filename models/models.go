package models

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
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

// Validate performs validation of Event fields
func (e *Event) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("event ID cannot be empty")
	}
	if e.EntityType == "" {
		return fmt.Errorf("entity type cannot be empty")
	}
	if e.EventName == "" {
		return fmt.Errorf("event name cannot be empty")
	}
	if e.SessionID == "" {
		return fmt.Errorf("session ID cannot be empty")
	}
	return nil
}

// String returns a string representation of the Event
func (e *Event) String() string {
	return fmt.Sprintf("Event{ID: %s, Type: %s, Name: %s, Session: %s}",
		e.ID, e.EntityType, e.EventName, e.SessionID)
}

// EventRecovery represents a document in the new_event_recovery collection
type EventRecovery struct {
	ID     primitive.ObjectID `bson:"_id,omitempty"`
	Events []Event            `bson:"event"`
}

// Validate performs validation of EventRecovery fields
func (er *EventRecovery) Validate() error {
	if len(er.Events) == 0 {
		return fmt.Errorf("events array cannot be empty")
	}
	for i, event := range er.Events {
		if err := event.Validate(); err != nil {
			return fmt.Errorf("invalid event at index %d: %v", i, err)
		}
	}
	return nil
}

// Result represents the validation result for a single event
type Result struct {
	EventID        string
	EntityType     string
	FoundInDest    bool
	CollectionName string
	Error          error
	Event          Event
	OffsetID       int
}

// IsSuccess returns true if the validation was successful and the event was found
func (r *Result) IsSuccess() bool {
	return r.Error == nil && r.FoundInDest
}

// String returns a string representation of the Result
func (r *Result) String() string {
	status := "✅ Found"
	if r.Error != nil {
		status = fmt.Sprintf("❌ Error: %v", r.Error)
	} else if !r.FoundInDest {
		status = "❌ Not Found"
	}
	return fmt.Sprintf("Result{Event: %s, Collection: %s, Status: %s}",
		r.EventID, r.CollectionName, status)
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

// Validate performs validation of MySQLEvent fields
func (me *MySQLEvent) Validate() error {
	if me.EventName == "" {
		return fmt.Errorf("event name cannot be empty")
	}
	if me.SessionID == "" {
		return fmt.Errorf("session ID cannot be empty")
	}
	if me.TrackID == "" {
		return fmt.Errorf("track ID cannot be empty")
	}
	return nil
}

// String returns a string representation of the MySQLEvent
func (me *MySQLEvent) String() string {
	return fmt.Sprintf("MySQLEvent{ID: %d, Name: %s, Session: %s, Track: %s}",
		me.ID, me.EventName, me.SessionID, me.TrackID)
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

// IsSuccess returns true if the MySQL validation was successful and the event was found
func (mr *MySQLEventResult) IsSuccess() bool {
	return mr.Error == nil && mr.Found
}

// String returns a string representation of the MySQLEventResult
func (mr *MySQLEventResult) String() string {
	status := "✅ Found"
	if mr.Error != nil {
		status = fmt.Sprintf("❌ Error: %v", mr.Error)
	} else if !mr.Found {
		status = "❌ Not Found"
	}
	return fmt.Sprintf("MySQLResult{Event: %s, Collection: %s, Status: %s}",
		mr.EventID, mr.CollectionName, status)
}

// CombinedResult combines MongoDB and MySQL results
type CombinedResult struct {
	MongoResult Result
	MySQLResult *MySQLEventResult
}

// IsSuccess returns true if both MongoDB and MySQL validations were successful
func (cr *CombinedResult) IsSuccess() bool {
	return cr.MongoResult.IsSuccess() &&
		(cr.MySQLResult == nil || cr.MySQLResult.IsSuccess())
}

// String returns a string representation of the CombinedResult
func (cr *CombinedResult) String() string {
	mysqlStatus := "N/A"
	if cr.MySQLResult != nil {
		if cr.MySQLResult.Error != nil {
			mysqlStatus = fmt.Sprintf("Error: %v", cr.MySQLResult.Error)
		} else {
			mysqlStatus = fmt.Sprintf("Found: %v", cr.MySQLResult.Found)
		}
	}
	return fmt.Sprintf("CombinedResult{Mongo: %v, MySQL: %s}",
		cr.MongoResult.String(), mysqlStatus)
}

// MissingDataReport stores information about missing events
type MissingDataReport struct {
	Timestamp         string                    `json:"timestamp"`
	TotalCount        int                       `json:"total_count"`
	ByCollection      map[string][]MissingEvent `json:"by_collection"`
	MySQLMissingCount int                       `json:"mysql_missing_count"`
	MySQLMissing      []MySQLMissingEvent       `json:"mysql_missing_events"`
	Errors            []string                  `json:"errors,omitempty"`
}

// AddError adds an error to the report
func (r *MissingDataReport) AddError(err error) {
	if err != nil {
		r.Errors = append(r.Errors, err.Error())
	}
}

// AddMissingEvent adds a missing event to the appropriate collection
func (r *MissingDataReport) AddMissingEvent(event MissingEvent) {
	if r.ByCollection == nil {
		r.ByCollection = make(map[string][]MissingEvent)
	}
	r.ByCollection[event.EntityType] = append(r.ByCollection[event.EntityType], event)
	r.TotalCount++
}

// AddMySQLMissingEvent adds a missing MySQL event to the report
func (r *MissingDataReport) AddMySQLMissingEvent(event MySQLMissingEvent) {
	r.MySQLMissing = append(r.MySQLMissing, event)
	r.MySQLMissingCount++
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

// String returns a string representation of the MissingEvent
func (me *MissingEvent) String() string {
	return fmt.Sprintf("MissingEvent{ID: %s, Type: %s, Name: %s, Session: %s}",
		me.ID, me.EntityType, me.EventName, me.SessionID)
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

// String returns a string representation of the MySQLMissingEvent
func (me *MySQLMissingEvent) String() string {
	return fmt.Sprintf("MySQLMissingEvent{ID: %s, Name: %s, Required: %v}",
		me.ID, me.EventName, me.RequiredInDB)
}

// EnhancedMissingDataReport represents an enhanced report structure with detailed metrics
type EnhancedMissingDataReport struct {
	Timestamp                string                     `json:"timestamp"`
	TotalCount               int                        `json:"total_count"`
	MySQLMissingCount        int                        `json:"mysql_missing_count"`
	MongoToMySQLMissingCount int                        `json:"mongo_to_mysql_missing_count"`
	ByCollection             map[string][]MissingEvent  `json:"by_collection"`
	MySQLMissing             []MySQLMissingEvent        `json:"mysql_missing_events,omitempty"`
	MongoToMySQLMissing      []MongoToMySQLMissingEvent `json:"mongo_to_mysql_missing,omitempty"`
	Errors                   []string                   `json:"errors,omitempty"`
	CollectionSummary        map[string]CollectionStats `json:"collection_summary"`
	MongoMySQLSummary        MongoMySQLStats            `json:"mongo_mysql_summary"`
}

// AddError adds an error to the enhanced report
func (r *EnhancedMissingDataReport) AddError(err error) {
	if err != nil {
		r.Errors = append(r.Errors, err.Error())
	}
}

// AddMissingEvent adds a missing event to the appropriate collection in the enhanced report
func (r *EnhancedMissingDataReport) AddMissingEvent(event MissingEvent, collection string) {
	if r.ByCollection == nil {
		r.ByCollection = make(map[string][]MissingEvent)
	}
	r.ByCollection[collection] = append(r.ByCollection[collection], event)
	r.TotalCount++
}

// MongoToMySQLMissingEvent represents an event that exists in MongoDB but is missing in MySQL
type MongoToMySQLMissingEvent struct {
	ID             string      `json:"id"`
	EventName      string      `json:"event_name"`
	CollectionName string      `json:"collection_name"`
	SessionID      string      `json:"session_id"`
	OffsetID       int         `json:"offset_id"`
	EntityCode     interface{} `json:"entity_code"`
}

// String returns a string representation of the MongoToMySQLMissingEvent
func (me *MongoToMySQLMissingEvent) String() string {
	return fmt.Sprintf("MongoToMySQLMissingEvent{ID: %s, Name: %s, Collection: %s}",
		me.ID, me.EventName, me.CollectionName)
}

// CollectionStats represents statistics for each collection
type CollectionStats struct {
	TotalEvents           int `json:"total_events"`
	MissingInMongo        int `json:"missing_in_mongo"`
	MissingInMySQL        int `json:"missing_in_mysql"`
	RequiredInMySQL       int `json:"required_in_mysql"`
	MissingButRequiredSQL int `json:"missing_but_required_sql"`
}

// AddMissingInMongo increments the count of events missing in MongoDB
func (cs *CollectionStats) AddMissingInMongo() {
	cs.MissingInMongo++
}

// AddMissingInMySQL increments the count of events missing in MySQL
func (cs *CollectionStats) AddMissingInMySQL() {
	cs.MissingInMySQL++
}

// MongoMySQLStats represents overall MongoDB to MySQL synchronization statistics
type MongoMySQLStats struct {
	TotalEventsProcessed         int `json:"total_events_processed"`
	MissingInMongo               int `json:"missing_in_mongo"`
	MissingInMySQL               int `json:"missing_in_mysql"`
	PresentInMongoMissingInMySQL int `json:"present_in_mongo_missing_in_mysql"`
}

// UpdateStats updates the MongoDB to MySQL synchronization statistics
func (ms *MongoMySQLStats) UpdateStats(missingInMongo, missingInMySQL, presentInMongoMissingInMySQL int) {
	ms.MissingInMongo += missingInMongo
	ms.MissingInMySQL += missingInMySQL
	ms.PresentInMongoMissingInMySQL += presentInMongoMissingInMySQL
	ms.TotalEventsProcessed++
}

// String returns a string representation of the MongoMySQLStats
func (ms *MongoMySQLStats) String() string {
	return fmt.Sprintf("Stats{Total: %d, MissingMongo: %d, MissingMySQL: %d, MongoNotMySQL: %d}",
		ms.TotalEventsProcessed, ms.MissingInMongo, ms.MissingInMySQL,
		ms.PresentInMongoMissingInMySQL)
}

// SummaryReport represents a summary of all event processing
type SummaryReport struct {
	Timestamp          string `json:"timestamp"`
	TotalEventsChecked int    `json:"total_events_checked"`
	MongoMissingCount  int    `json:"mongo_missing_count"`
	MySQLMissingCount  int    `json:"mysql_missing_count"`
	ErrorCount         int    `json:"error_count"`
}

// MongoToSQLReport represents events that exist in MongoDB but are missing in MySQL
type MongoToSQLReport struct {
	Timestamp  string                     `json:"timestamp"`
	TotalCount int                        `json:"total_count"`
	Events     []MongoToMySQLMissingEvent `json:"events"`
}

// MongoReport represents events missing in MongoDB
type MongoReport struct {
	Timestamp  string         `json:"timestamp"`
	Events     []MissingEvent `json:"events"`
	TotalCount int            `json:"total_count"`
}

// MySQLReport represents events missing in MySQL
type MySQLReport struct {
	Timestamp  string                     `json:"timestamp"`
	Events     []MongoToMySQLMissingEvent `json:"events"`
	TotalCount int                        `json:"total_count"`
}

// ErrorReport represents detailed error information for each event
type ErrorReport struct {
	Timestamp  string       `json:"timestamp"`
	Errors     []ErrorEvent `json:"errors"`
	TotalCount int          `json:"total_count"`
}

// ErrorEvent represents error information for a single event
type ErrorEvent struct {
	ID         string `json:"id"`
	EventName  string `json:"event_name"`
	EntityType string `json:"entity_type"`
	SessionID  string `json:"session_id"`
	ErrorType  string `json:"error_type"` // "mongo" or "mysql"
	ErrorMsg   string `json:"error_message"`
}
