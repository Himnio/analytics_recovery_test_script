package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"strings"
	"time"

	"analytics/config"
	"analytics/db"
	"analytics/models"
	"analytics/report"
	"analytics/validator"

	"go.mongodb.org/mongo-driver/mongo"
)

const (
	mongoDisconnectTimeout = 10 * time.Second
	eventMappingCSVPath    = "./csv/Docquity1.0.csv"
)

// main is the entry point of the application that coordinates the data recovery and validation process
func main() {
	// Initialize logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Parse configuration
	cfg := config.ParseFlags()
	printConfiguration(cfg)

	// Initialize database connections and defer cleanup
	mongoClient, mongoDB := initializeMongoDB(cfg)
	defer cleanupMongoDB(mongoClient)

	var mysqlDB *sql.DB
	if cfg.EnableMySQL {
		mysqlDB = initializeMySQL(cfg)
		defer mysqlDB.Close()
	}

	// Process event recoveries
	processEventRecoveries(cfg, mongoDB, mysqlDB)
}

// initializeMongoDB establishes connection to MongoDB and returns both client and database
func initializeMongoDB(cfg *config.Configuration) (*mongo.Client, *mongo.Database) {
	client, err := db.ConnectMongoDB(cfg.MongoURI, cfg.ConnectionTimeout)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	return client, client.Database(cfg.DatabaseName)
}

// cleanupMongoDB gracefully disconnects from MongoDB
func cleanupMongoDB(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), mongoDisconnectTimeout)
	defer cancel()
	if err := client.Disconnect(ctx); err != nil {
		log.Printf("Error disconnecting from MongoDB: %v", err)
	}
}

// initializeMySQL establishes connection to MySQL and returns the database handle
func initializeMySQL(cfg *config.Configuration) *sql.DB {
	mysqlConfig := db.DefultMySQLConfig()

	// Validate and set MySQL DSN
	if isEmptyDSN(mysqlConfig.DSN) {
		if cfg.MySQLDSN != "" {
			log.Println("Using MySQL DSN from command line arguments")
			mysqlConfig.DSN = cfg.MySQLDSN
		} else {
			log.Fatal("MySQL DSN is empty. Please set DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME environment variables or provide a DSN via -mysql-dsn flag")
		}
	}

	mysqlDB, err := db.ConnectMySQL(mysqlConfig)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	return mysqlDB
}

// isEmptyDSN checks if the MySQL DSN is empty or invalid
func isEmptyDSN(dsn string) bool {
	return dsn == ":@tcp(:)/" || strings.HasPrefix(dsn, "@tcp")
}

// processEventRecoveries handles the main business logic of processing and validating events
func processEventRecoveries(
	cfg *config.Configuration,
	mongoDB *mongo.Database,
	mysqlDB *sql.DB,
) {
	// Query event_recovery collection
	eventRecoveries, err := db.GetEventRecoveries(mongoDB, cfg.CollectionName, cfg.DocLimit, cfg.QueryTimeout)
	if err != nil {
		log.Fatalf("Failed to get event recoveries: %v", err)
	}

	log.Printf("Processing %d event recovery documents", len(eventRecoveries))

	if cfg.EnableMySQL {
		results := processWithMySQL(mongoDB, mysqlDB, eventRecoveries, cfg)
		report.CreateEnhancedMissingDataReport(results)
	} else {
		results := processMongoOnly(mongoDB, eventRecoveries, cfg)
		report.CreateMissingDataReport(results)
	}
}

// processWithMySQL handles event validation with both MongoDB and MySQL
func processWithMySQL(
	mongoDB *mongo.Database,
	mysqlDB *sql.DB,
	eventRecoveries []models.EventRecovery,
	cfg *config.Configuration,
) []models.CombinedResult {
	var allResults []models.CombinedResult

	for i, recovery := range eventRecoveries {
		log.Printf("Processing document %d/%d with %d events...",
			i+1, len(eventRecoveries), len(recovery.Events))

		results := validator.ProcessEventsWithMySQL(
			mongoDB,
			mysqlDB,
			recovery.Events,
			cfg.QueryTimeout,
			cfg.MaxConcurrent,
			eventMappingCSVPath,
			i+1,
		)
		allResults = append(allResults, results...)
	}

	log.Printf("Finished processing %d documents with a total of %d events",
		len(eventRecoveries), len(allResults))

	return allResults
}

// processMongoOnly handles event validation with MongoDB only
func processMongoOnly(
	mongoDB *mongo.Database,
	eventRecoveries []models.EventRecovery,
	cfg *config.Configuration,
) []models.Result {
	var allResults []models.Result

	for i, recovery := range eventRecoveries {
		log.Printf("Processing document %d/%d with %d events",
			i+1, len(eventRecoveries), len(recovery.Events))

		results := validator.ProcessEventsInDocument(
			mongoDB,
			recovery.Events,
			cfg.QueryTimeout,
			cfg.MaxConcurrent,
			i+1,
		)
		allResults = append(allResults, results...)
	}

	return allResults
}

// printConfiguration outputs the current configuration settings
func printConfiguration(cfg *config.Configuration) {
	fmt.Printf("\nConfiguration:\n")
	fmt.Printf("  MongoDB URI: %s\n", cfg.MongoURI)
	fmt.Printf("  Database: %s\n", cfg.DatabaseName)
	fmt.Printf("  Collection: %s\n", cfg.CollectionName)
	fmt.Printf("  Query Timeout: %d seconds\n", cfg.QueryTimeout)
	fmt.Printf("  Connection Timeout: %d seconds\n", cfg.ConnectionTimeout)
	fmt.Printf("  Max Concurrent Operations: %d\n", cfg.MaxConcurrent)
	fmt.Printf("  MySQL Enabled: %v\n", cfg.EnableMySQL)
	if cfg.EnableMySQL {
		fmt.Printf("  MySQL DSN: %s\n", cfg.MySQLDSN)
	}
	fmt.Printf("  GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("  NumCPU: %d\n", runtime.NumCPU())
	fmt.Printf("  Document Limit: %s\n\n",
		func() string {
			if cfg.DocLimit > 0 {
				return fmt.Sprintf("%d", cfg.DocLimit)
			}
			return "No limit (processing all documents)"
		}(),
	)
}
