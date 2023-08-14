// Package producer contains produce functions
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
	"github.com/sirupsen/logrus"
)

// constants for message
const (
	MIN = 100
	MAX = 1000
)

// random function generates a random number between min & max
func random(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
}

// Message struct for pushing messages to RabbitMQ
type Message struct {
	ID        uuid.UUID
	RandomInt int
}

// Produce function Produces messages to RabbitMQ
func Produce(connString string, limit int) {
	conn, err := amqp.Dial(connString)
	if err != nil {
		logrus.Errorf("Dial: %v", err)
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
	msgCount := 0
	start := time.Now()
	for time.Since(start) < time.Second && msgCount < limit {
		msg := Message{
			ID:        uuid.New(),
			RandomInt: random(MIN, MAX),
		}
		recordJSON, _ := json.Marshal(msg)
		err = ch.PublishWithContext(context.TODO(),
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        recordJSON,
			})
		if err != nil {
			logrus.Errorf("PublishWithContext: %v", err)
		}
		log.Printf("%d -> %v : %v\n", msgCount, msg.ID, msg.RandomInt)
		msgCount++
	}
	fmt.Println("time, spent on producing: ", time.Since(start).Seconds())
	fmt.Println("sent messages: ", msgCount)
}
