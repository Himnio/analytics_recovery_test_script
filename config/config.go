package config

import (
	"flag"
	"log"
	"os"

	"github.com/joho/godotenv"
)

// Configuration holds all the configurable parameters
type Configuration struct {
	MongoURI          string
	DatabaseName      string
	DocLimit          int
	CollectionName    string
	QueryTimeout      int
	ConnectionTimeout int
	MaxConcurrent     int
}

// ParseFlags parses command-line flags and returns a Configuration
func ParseFlags() *Configuration {
	config := &Configuration{}

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading env")
	}

	mongo_url := os.Getenv("MONGO_DB_URL")
	db_name := os.Getenv("DATABASE_NAME")

	flag.StringVar(&config.MongoURI, "mongo-uri", mongo_url, "MongoDB connection URI")
	flag.StringVar(&config.DatabaseName, "db", db_name, "MongoDB database name")
	flag.IntVar(&config.DocLimit, "limit", 0, "Limit the number of documents to process (0 = process all)")
	flag.StringVar(&config.CollectionName, "collection", "new_event_recovery", "Source collection name")
	flag.IntVar(&config.QueryTimeout, "query-timeout", 15, "Query timeout in seconds")
	flag.IntVar(&config.ConnectionTimeout, "conn-timeout", 30, "Connection timeout in seconds")
	flag.IntVar(&config.MaxConcurrent, "max-concurrent", 10, "Maximum number of concurrent operations")

	// Parse command-line flags
	flag.Parse()

	return config
}
