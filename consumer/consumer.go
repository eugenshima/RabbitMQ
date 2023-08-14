// Package consumer contains consume functions and database operations
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// NewDBPsql function provides Connection with PostgreSQL database
func NewDBPsql() (*pgxpool.Pool, error) {
	// Initialization a connect configuration for a PostgreSQL using pgx driver
	config, err := pgxpool.ParseConfig("postgres://eugen:ur2qly1ini@localhost:5432/eugene")
	if err != nil {
		return nil, fmt.Errorf("error connection to PostgreSQL: %v", err)
	}

	// Establishing a new connection to a PostgreSQL database using the pgx driver
	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("error connection to PostgreSQL: %v", err)
	}
	// Output to console
	fmt.Println("Connected to PostgreSQL!")

	return pool, nil
}

// Message struct for consuming messages from RabbitMQ
type Message struct {
	ID        uuid.UUID
	RandomInt int
}

// Consume function consumes messages from RabbitMQ
func Consume(connString string, limit int) {
	pool, err := NewDBPsql()
	if err != nil {
		logrus.Errorf("NewDBPsql: %v", err)
	}
	defer pool.Close()

	conn, err := amqp.Dial(connString)
	if err != nil {
		logrus.WithFields(logrus.Fields{"connString:": connString}).Errorf("Dial: %v", err)
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			logrus.Errorf("Close: %v", err)
			return
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		logrus.Errorf("Channel: %v", err)
	}
	defer func() {
		err = ch.Close()
		if err != nil {
			logrus.Errorf("Close: %v", err)
			return
		}
	}()

	q, err := ch.QueueDeclare(
		"hello1", // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		logrus.Errorf("QueueDeclare: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		logrus.Errorf("Consume: %v", err)
	}

	msgCount := 0
	temp := &Message{}
	start := time.Now()
	batch := &pgx.Batch{}

	for d := range msgs {
		log.Printf("Received a message: [%d] -> %s", msgCount, d.Body)
		err = json.Unmarshal(d.Body, &temp)
		if err != nil {
			logrus.WithFields(logrus.Fields{"Body:": d.Body, "temp:": &temp}).Errorf("Unmarshal: %v", err)
		}

		batch.Queue("INSERT INTO kafka.kafka_storage (id, kafka_message) VALUES ($1, $2)", temp.ID, temp.RandomInt)
		batch.Queue("Update kafka.kafka_storage Set \"check\"=true WHERE id=$1", temp.ID)

		msgCount++
		if msgCount == limit || time.Since(start) > time.Second {
			break
		}
	}
	err = Insert(pool, batch)
	if err != nil {
		logrus.Errorf("Failed to Insert message: %v", err)
	}

	fmt.Println("time, spent on reading: ", time.Since(start).Seconds())
	fmt.Println("received messages: ", msgCount)
}

// Insert function executes SQL request to insert RabbitMQ message into database
func Insert(pool *pgxpool.Pool, batch *pgx.Batch) error {
	br := pool.SendBatch(context.Background(), batch)
	_, err := br.Exec()
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	return nil
}
