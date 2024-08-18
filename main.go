package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

// var csvFilePath = "/Users/pravinmenon/Documents/Coding/GO_Tutorial/Webhook code/data.csv"

var csvFilePath = "https://docs.google.com/spreadsheets/d/1e5fBizApsJuoiS8F_k5sjP3F03MaBI0kz63ByWMx8nk/edit?usp=sharing"

func main() {
	// Create a new Gin router
	router := gin.Default()
	// router.ForwardedByClientIP = true
	// router.SetTrustedProxies([]string{"127.0.0.1"})

	// Define a route for handling the webhook POST request
	router.POST("/webhook", HandleWebhook)

	// Start the server on port 8080
	if err := router.Run(":8080"); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func HandleWebhook(c *gin.Context) {
	var data map[string]interface{}
	if err := c.ShouldBindJSON(&data); err != nil {
		fmt.Println("Error:", err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}

	// Convert Ts and TsEvent to string if they exist
	if ts, ok := data["ts"]; ok {
		data["ts"] = fmt.Sprintf("%v", ts)
	}
	if tsevent, ok := data["ts_event"]; ok {
		data["ts_event"] = fmt.Sprintf("%v", tsevent)
	}

	if err := writeToCSV(data); err != nil {
		fmt.Println("Error:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to write to CSV"})
		return
	}

	c.String(http.StatusOK, "Post request received")
}

func writeToCSV(data map[string]interface{}) error {
	file, err := os.OpenFile(csvFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var record []string
	if data["event"] == "Campaign sent" {
		record = []string{
			fmt.Sprintf("%v", data["campaign_id"]),
			fmt.Sprintf("%v", data["campaign_name"]),
			fmt.Sprintf("%v", data["tag_name"]),
			fmt.Sprintf("%v", data["event"]),
			fmt.Sprintf("%v", data["date_sent"]),
			fmt.Sprintf("%v", data["ts"]),
			fmt.Sprintf("%v", data["ts_event"]),
		}
	} else {
		record = []string{
			fmt.Sprintf("%v", data["event"]),
			fmt.Sprintf("%v", data["email"]),
			getString(data, "URL"),
			getString(data, "list_id"),
			getString(data, "reason"),
			fmt.Sprintf("%v", data["campaign_name"]),
			fmt.Sprintf("%v", data["tag"]),
			fmt.Sprintf("%v", data["camp_id"]),
			fmt.Sprintf("%v", data["date_event"]),
			fmt.Sprintf("%v", data["ts"]),
			fmt.Sprintf("%v", data["ts_event"]),
		}
	}

	if err := writer.Write(record); err != nil {
		return err
	}

	return nil
}

// getString returns the string representation of the value in the map for the given key,
// or an empty string if the key does not exist.
func getString(data map[string]interface{}, key string) string {
	if value, ok := data[key]; ok {
		return fmt.Sprintf("%v", value)
	}
	return ""
}
