package main

import (
	"github.com/eugenshima/RabbitMQ/config"
	"github.com/eugenshima/RabbitMQ/consumer"
)

func main() {
	connString := config.ConstConn
	consumer.Consume(connString)
}
