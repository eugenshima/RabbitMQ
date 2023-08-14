package consumer

import (
	"context"
	"fmt"
	"log"
	"time"

	"encoding/json"

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

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

type Message struct {
	Id        uuid.UUID
	RandomInt int
}

func Consume(connString string, limit int) {
	pool, err := NewDBPsql()
	if err != nil {
		logrus.Errorf("NewDBPsql: %v", err)
	}
	defer pool.Close()

	conn, err := amqp.Dial(connString)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello1", // name
		false,    // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")
	msgCount := 0

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	temp := &Message{}
	var rows [][]interface{}
	start := time.Now()

	for d := range msgs {
		log.Printf("Received a message: [%d] -> %s", msgCount, d.Body)
		err := json.Unmarshal(d.Body, &temp)
		if err != nil {
			logrus.WithFields(logrus.Fields{"Body:": d.Body, "temp:": &temp}).Errorf("Unmarshal: %v", err)
		}
		rows = append(rows, []interface{}{
			temp.Id,
			temp.RandomInt,
		})
		msgCount++
		if msgCount == limit {
			break
		}

		if time.Since(start) > time.Second {
			break
		}
	}
	fmt.Println("time, spent on reading: ", time.Since(start).Seconds())
	err = Create(context.Background(), pool, rows)
	if err != nil {
		logrus.Errorf("Create: %v", err)
	}
	fmt.Println("received messages: ", msgCount)
}

// Create function executes SQL request to insert RabbitMQ message into database
func Create(ctx context.Context, pool *pgxpool.Pool, rows [][]interface{}) error {
	_, err := pool.CopyFrom(
		ctx,
		pgx.Identifier{"kafka", "kafka_storage"},
		[]string{"id", "kafka_message"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("CopyFrom: %W", err)
	}

	fmt.Println()
	return nil
}
