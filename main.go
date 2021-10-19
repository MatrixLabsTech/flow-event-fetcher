/**
 * main.go
 * Copyright (c) 2021 Alvin(Xinyao) Sun <asun@whitematrix.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/Lucklyric/ultimate-flow-event-fetcher/spork"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

var url string = ""

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

func queryEventByBlockRange(c *gin.Context) {
	event := c.PostForm("event")
	start, _ := strconv.ParseInt(c.PostForm("start"), 10, 64)
	end, _ := strconv.ParseInt(c.PostForm("end"), 10, 64)
	log.Info(fmt.Sprintf("query %s, from %d to %d", event, start, end))

	ret, err := sporkStore.QueryEventByBlockRange(event, uint64(start), uint64(end))
	if err != nil {
		log.Error(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	jsonRet := spork.BlockEventsToJSON(ret)
	log.Info(fmt.Sprintf("Got %d events", len(jsonRet)))
	c.JSON(http.StatusOK, jsonRet)

}

func init() {
	url := os.Getenv("SPORK_JSON_URL")
	if url == "" {
		url = "https://raw.githubusercontent.com/Lucklyric/flow-spork-info/main/spork.json"
	}
	sporkStore = spork.New(url)

}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8989"
	}
	router := gin.Default()

	router.GET("/version", version)
	router.POST("/queryEventByBlockRange", queryEventByBlockRange)

	log.Info("Starting server...")
	router.Run(":" + port)
}
