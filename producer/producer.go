package main

import (
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `from:"text" json:"text"`
}

func main() {
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	app.Listen(":3000")
}

func ConnectProducer(brokersURL []string) (map[string]*sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersURL, config)
	if err != nil {
		log.Panicf("Error creating producer: %v", err)
		return nil, err
	}
	producers := make(map[string]*sarama.SyncProducer)
	producers["default"] = &conn
	return producers, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokersURL := []string{"localhost:9092"}
	producer, err := ConnectProducer(brokersURL)
	if err != nil {
		log.Panicf("Error creating producer: %v", err)
		return err
	}
	defer (*producer["default"]).Close()
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition , offset, err := (*producer["default"]).SendMessage(msg)
	if err!= nil {
		log.Panicf("Error pushing message: %v", err)
		return err
	}
	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func createComment(c *fiber.Ctx) error {
	cmt := new(Comment)
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": "Cannot parse JSON",
			"error":   err,
		})
		return err
	}
	cmtInBytes, err := json.Marshal(cmt)
	PushCommentToQueue(cmtInBytes)

	err = c.Status(200).JSON(&fiber.Map{
		"success": true,
		"message": "Comment Pushed Successfully",
		"comment": cmt,
	})
	if err != nil {
		log.Println(err)
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error Creating Product",
			"error":   err,
		})
		return err
	}
	return err
}
