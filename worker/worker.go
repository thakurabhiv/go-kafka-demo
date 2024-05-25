package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

const (
  topic = "comments"
)

func main() {
  brokerUrls := []string { "localhost:9092" }

  worker, err := connectConsumer(brokerUrls)
  if err != nil {
    panic(err)
  }

  consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
  if err != nil {
  panic(err)
  }

  log.Println("Consumer started")
  sigChan := make(chan os.Signal, 1)
  signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

  msgCount := 0
  doneCh := make(chan struct{})

  go func() {
    for {
      select {
      case err := <- consumer.Errors():
        log.Println(err)
      case msg := <- consumer.Messages():
        msgCount++
        log.Printf("Received message count %d | Topic(%s) | Message(%s)\n", msgCount, msg.Topic, msg.Value)
      case <-sigChan:
        log.Println("Interrupted process")
        doneCh <- struct{}{}
      }
    }
  }()

  <-doneCh

  log.Printf("Processed %d messages", msgCount)

  if err = consumer.Close(); err != nil {
    panic(err)
  }
}

func connectConsumer(brokerUrls []string) (sarama.Consumer, error) {
  config := sarama.NewConfig()
  config.Consumer.Return.Errors = true
  config.Version = sarama.V0_11_0_0

  conn, err := sarama.NewConsumer(brokerUrls, config)
  if err != nil {
    return nil, err
  }

  return conn, nil
}

