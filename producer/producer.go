package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

// random function generates a random number between min & max
func random(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
}

type Message struct {
	Id        uuid.UUID
	RandomInt int
}

func Produce(connString string, limit int) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	msgCount := 0
	start := time.Now()
	for time.Since(start) < time.Second && msgCount < limit {
		msg := Message{
			Id:        uuid.New(),
			RandomInt: random(100, 1000),
		}
		recordJSON, _ := json.Marshal(msg)
		err = ch.PublishWithContext(ctx,
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        recordJSON,
			})
		failOnError(err, "Failed to publish a message")
		log.Printf("%d -> %v : %v\n", msgCount, msg.Id, msg.RandomInt)
		msgCount++
	}
	fmt.Println("time, spent on producing: ", time.Since(start).Seconds())
	fmt.Println("sent messages: ", msgCount)
}
