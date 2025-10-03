package main

import (
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

// Comment model
type Comment struct {
	Text string `form:"text" json:"text"`
}

// Global Kafka producer
var producer sarama.SyncProducer

func main() {
	// Connect Kafka producer once
	var err error
	producer, err = ConnectProducer([]string{"localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}
	defer producer.Close()

	// Fiber app
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)

	log.Println("🚀 Server running on http://localhost:3000")
	if err := app.Listen(":3000"); err != nil {
		log.Fatal(err)
	}
}

// ConnectProducer creates a Kafka producer
func ConnectProducer(brokersURL []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersURL, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// PushCommentToQueue sends a message to a Kafka topic
func PushCommentToQueue(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Printf("✅ Message stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

// createComment handles POST /comments
func createComment(c *fiber.Ctx) error {
	cmt := new(Comment)
	if err := c.BodyParser(cmt); err != nil {
		log.Println("❌ BodyParser error:", err)
		return c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": "Cannot parse JSON",
			"error":   err.Error(),
		})
	}

	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		log.Println("❌ Marshal error:", err)
		return c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error encoding comment",
			"error":   err.Error(),
		})
	}

	if err := PushCommentToQueue("comments", cmtInBytes); err != nil {
		log.Println("❌ Kafka push error:", err)
		return c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Failed to push comment to Kafka",
			"error":   err.Error(),
		})
	}

	return c.Status(200).JSON(&fiber.Map{
		"success": true,
		"message": "Comment Pushed Successfully",
		"comment": cmt,
	})
}
