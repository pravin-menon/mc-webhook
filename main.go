package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"os"
	"context"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
	"github.com/gin-gonic/gin"
)

// var csvFilePath = "/Users/pravinmenon/Documents/Coding/GO_Tutorial/Webhook code/data.csv"

var csvFilePath = "https://storage.googleapis.com/mc-webhook-data/data.csv"

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
	ctx := context.Background()

    // Replace with the path to your service account key file
    keyFilePath := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")

    client, err := storage.NewClient(ctx, option.WithCredentialsFile(keyFilePath))
    if err != nil {
         log.Fatal((err))
    }
    defer client.Close()

	bucketName := "mc-webhook-data"
	// objectName := csvFilePath

	// Download the file
	rc, err := client.Bucket(bucketName).Object(csvFilePath).NewReader(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	// Write the file contents to a local temporary file
	tmpFile, err := os.CreateTemp("", "gcs_csv_")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, rc); err != nil {
		log.Fatal(err)
	}

	file, err := os.OpenFile(tmpFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

	wc := client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
	defer wc.Close()

	f, err := os.Open(tmpFile.Name())
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if _, err := io.Copy(wc, f); err != nil {
		log.Fatal(err)
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
