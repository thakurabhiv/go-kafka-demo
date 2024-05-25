package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

const (
  topic = "comments"
)

type Comment struct {
  Text string `form:"text" json:"text"`
}

func main() {
  app := fiber.New()

  api := app.Group("/api/v1")
  api.Post("/comment", createComment)
  api.Get("/bulkComments", createBulkCommentsAsync)

  app.Listen(":9000")
}

func createBulkCommentsAsync(c *fiber.Ctx) error {
  go func() {
    for i := 0; i < 10; i++ {
      cmt := Comment {
        Text: fmt.Sprintf("Comment #%v", i+1),
      }

      cmtBytes, err := json.Marshal(cmt)
      if err != nil {
        log.Printf("Error marshaling comment: %v", err)
      }

      pushCommentToQueue(topic, cmtBytes)
      log.Println("Comment pushed to queue")
      time.Sleep(time.Second)
    }
  }()


  c.Status(http.StatusOK).JSON(&fiber.Map{
    "status": true,
    "message": "Started pushing comments in background",
  })

  return nil
}

func createComment(c *fiber.Ctx) error {
  cmt := new(Comment)

  if err := c.BodyParser(&cmt); err != nil {
    log.Printf("Error while parsing comment: %v", err)
    c.Status(400).JSON(&fiber.Map{
      "status": false,
      "message": err,
    })

    return err
  }

  cmtbytes, err := json.Marshal(cmt)
  if err != nil {
    log.Printf("Error while marshalling comment: %v", err)
    return err
  }

  pushCommentToQueue(topic, cmtbytes)

  c.Status(http.StatusOK).JSON(&fiber.Map {
    "success": true,
    "message": "comment pushed successfully",
    "comment": cmt,
  })

  return nil
}

func pushCommentToQueue(topic string, message []byte) error {
  brokerUrls := []string { "localhost:9092" }
  producer, err := connectProducer(brokerUrls)
  if err != nil {
    return err
  }

  defer producer.Close()

  msg := &sarama.ProducerMessage{
    Topic: topic,
    Value: sarama.StringEncoder(message),
  }

  partition, offset, err := producer.SendMessage(msg)
  if err != nil {
    log.Printf("Error while sending message to producer: %v", err)
  }

  log.Printf("Message is stored in Topic(%s)/Partition(%d)/Offset(%d)", topic, partition, offset)

  return nil
}

func connectProducer(brokerUrls []string) (sarama.SyncProducer, error) {
  config := sarama.NewConfig()
  config.Producer.Return.Successes = true
  config.Producer.RequiredAcks = sarama.WaitForAll
  config.Producer.Retry.Max = 5

  conn, err := sarama.NewSyncProducer(brokerUrls, config)
  if err != nil {
    return nil, err
  }

  return conn, nil
}
