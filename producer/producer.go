package main

import (
	"encoding/json"
	"log"

	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `from:"text" json:"text"`
}

func main(){
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	app.Listen(":3000")
}

func createComment(c *fiber.Ctx) error {
	cmt := new(Comment) 
	if err := c.BodyParser(cmt); err != nil{
		log.Println(err)
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": "Cannot parse JSON",
			"error": err,
		})
		return err
	}
	cmtInBytes , err := json.Marshal(cmt)
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
			"error": err,
		})
		return err
	}
	return err
}