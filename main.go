package main

import (
	"fmt"
	"matrixlab/ultimate-flow-event-fetcher/spork"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk/client"
	log "github.com/sirupsen/logrus"
)

var url string = "https://raw.githubusercontent.com/Lucklyric/flow-spork-info/main/spork.json"

var sporkStore *spork.SporkStore = nil

func version(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"version": "0.1.0"})
}

func callSyncSpork(c *gin.Context) {
	err := sporkStore.SyncSpork()
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
	}
}

type EventResult struct {
	BlockEvents []client.BlockEvents
}

func eventToJSON(e *cadence.Event) interface{} {
	preparedFields := make([]interface{}, 0)
	for i, field := range e.EventType.Fields {
		value := e.Fields[i]
		preparedFields = append(preparedFields,
			map[string]interface{}{
				"name":  field.Identifier,
				"value": value.String(),
			},
		)
	}
	return preparedFields
}

func blockEventsToJSON(e []client.BlockEvents) []interface{} {
	result := make([]interface{}, 0)

	for _, blockEvent := range e {
		if len(blockEvent.Events) > 0 {
			for _, event := range blockEvent.Events {
				result = append(result, map[string]interface{}{
					"blockID":       blockEvent.Height,
					"index":         event.EventIndex,
					"type":          event.Type,
					"transactionId": event.TransactionID.String(),
					"values":        eventToJSON(&(event.Value))})
			}
		}
	}

	return result
}

func queryEventByBlockRange(c *gin.Context) {
	event := c.PostForm("event")
	start, _ := strconv.ParseInt(c.PostForm("start"), 10, 64)
	end, _ := strconv.ParseInt(c.PostForm("end"), 10, 64)
	log.Info(fmt.Sprintf("query %s, from %d to %d", event, start, end))

	ret, err := sporkStore.QueryEventByBlockRange(event, uint64(start), uint64(end))
	if err != nil {
        log.Error(err.Error())
        c.JSON(http.StatusBadRequest, gin.H{"error":err.Error()})
        return
	}

    jsonRet := blockEventsToJSON(ret);
	log.Info(fmt.Sprintf("Got %d, events", len(jsonRet)))
	c.JSON(http.StatusOK, jsonRet)

}

func init() {
	sporkStore = spork.New(url)
}

func main() {
	router := gin.Default()

	router.GET("/version", version)
	router.POST("/queryEventByBlockRange", queryEventByBlockRange)

	log.Info("Starting server...")
	router.Run("0.0.0.0:" + os.Getenv("SERVERPORT"))
}
