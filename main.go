package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	gocqlastra "github.com/datastax/gocql-astra"
	"github.com/gocql/gocql"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/yaninyzwitty/kafka-consumer-go/model"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var session *gocql.Session

// var db *sql.DB
var reader *kafka.Reader
var ctx context.Context

func main() {
	ctx = context.Background()
	// load the environment variables
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file", err)
	}

	// Initialize Kafka consumer
	kafka_username := os.Getenv("KAFKA_USERNAME")
	kafka_passwd := os.Getenv("KAFKA_PASSWORD")
	bootstrap_server := os.Getenv("BOOTSTRAP_SERVER")
	mechanism, _ := scram.Mechanism(scram.SHA512, kafka_username, kafka_passwd)
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{bootstrap_server},
		GroupID: "products",
		Topic:   "products",
		Dialer: &kafka.Dialer{
			SASLMechanism: mechanism,
			TLS:           &tls.Config{},
		},
	})

	defer reader.Close()

	// CONNECTING TO CASSANDRA
	token := os.Getenv("CASSANDRA_CLIENT_TOKEN")
	cluster, err := gocqlastra.NewClusterFromBundle("./secure-connect.zip", "token", token, 10*time.Second)
	if err != nil {
		panic("Unable to load the astra bundle")
	}
	// creating cassandra session
	session, err = gocql.NewSession(*cluster)
	if err != nil {
		log.Fatalf("unable to create session: %v", err)
	}

	defer session.Close()

	// CONNECTING WITH MONGODB
	mongoURL := os.Getenv("MONGODB_URL")
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(mongoURL).SetServerAPIOptions(serverAPI)
	// Create a new client and connect to the server
	mongoDbClient, err := mongo.Connect(ctx, opts)
	if err != nil {
		log.Fatalf("error connecting to mongodb: %v", err)
	}

	defer func() {
		if err := mongoDbClient.Disconnect(ctx); err != nil {
			log.Fatalf("error disconnecting from mongodb: %v", err)
		}
	}()
	// optionally send a ping to confirm a successful connection

	// CONNECTING WITH POSTGRES
	connStr := os.Getenv("DATABASE_URL")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error opening database: %v", err)
	}
	defer db.Close()

	// Set up signal handler to gracefully stop the consumer
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Start consuming messages
	go consumeMessages()

	// Wait for termination signal
	<-sigchan

}

func consumeMessages() {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("Error consuming message: %v", err)
			continue
		}
		// Decode message data into Product struct

		var product model.Product
		if err := json.Unmarshal(message.Value, &product); err != nil {
			log.Printf("Error decoding message: %v", err)
			continue
		}
		// Process the product (e.g., write to database)
		if err := writeToDatabase(product); err != nil {
			log.Printf("Error writing to database: %v", err)
			continue
		}
		// Commit the message offset
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("Error committing message offset: %v", err)
			continue
		}
		log.Printf("Message offset committed: %s:%d", message.Topic, message.Partition)

	}
}

func writeToDatabase(product model.Product) error {
	err := session.Query("INSERT INTO chatsandra.products (product_id, name, description, price, quantity) VALUES (?, ?, ?, ?, ?)", product.ID, product.Name, product.Description, product.Price, product.Quantity).Exec()
	if err != nil {
		return fmt.Errorf("error inserting product into cassandra: %v", err)
	}
	log.Printf("Writing product to database: %+v", product)

	return nil

}
