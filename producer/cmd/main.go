package main

import (
	"github.com/eugenshima/RabbitMQ/config"
	"github.com/eugenshima/RabbitMQ/producer"
)

func main() {
	connString := config.ConstConn
	limit := config.ConstLimitMsg
	producer.Produce(connString, limit)
}
