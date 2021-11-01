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
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/MatrixLabTech/flow-event-fetcher/spork"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

type QueryEventByBlockRangeDto struct {
	Event string `json:"event"`
	Start int    `json:"start"`
	End   int    `json:"end"`
}

var url = ""

var sporkStore *spork.SporkStore = nil

func version(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"version": "0.1.0"})
}

func syncSpork(c *gin.Context) {
	err := sporkStore.SyncSpork()
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
	}
	c.JSON(http.StatusOK, gin.H{"spork": sporkStore.SporkList})
}

func queryLatestBlockHeight(c *gin.Context) {
	height, err := sporkStore.QueryLatestBlockHeight()
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
	}
	c.JSON(http.StatusOK, gin.H{"latestBlockHeight": height})
}

func queryEventByBlockRange(c *gin.Context) {
	var queryEventByBlockRangeDto QueryEventByBlockRangeDto
	err := c.Bind(&queryEventByBlockRangeDto)
	if err != nil {
		log.Error(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	log.Info(fmt.Sprintf("query %s, from %d to %d", queryEventByBlockRangeDto.Event, queryEventByBlockRangeDto.Start, queryEventByBlockRangeDto.End))

	ret, err := sporkStore.QueryEventByBlockRange(queryEventByBlockRangeDto.Event, uint64(queryEventByBlockRangeDto.Start), uint64(queryEventByBlockRangeDto.End))
	if err != nil {
		log.Error(err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	jsonRet := spork.BlockEventsToJSON(ret)
	log.Info(fmt.Sprintf("Got %d events", len(jsonRet)))
	c.JSON(http.StatusOK, jsonRet)

}

func main() {
	port := flag.String("port", "8686", "port to listen on")
	sporkUrl := flag.String("sporkUrl", "", "spork json url")
	maxQueryBlocks := flag.Uint64("maxQueryBlocks", 2000, "max query blocks")
	queryBatchSize := flag.Uint64("queryBatchSize", 200, "query batch size")
	flag.Parse()

	// check sporkUrl not empty
	if *sporkUrl == "" {
		log.Fatal("sporkUrl is empty")
	}

	sporkStore = spork.New(*sporkUrl, *maxQueryBlocks, *queryBatchSize)
	// display formatted sporkStore configuration
	log.Info(fmt.Sprintf("sporkStore configuration: %s", sporkStore.String()))

	// setup gin
	router := gin.Default()

	router.Use(gin.LoggerWithWriter(os.Stderr))

	router.GET("/version", version)
	router.GET("/syncSpork", syncSpork)
	router.POST("/queryEventByBlockRange", queryEventByBlockRange)
	router.GET("/queryLatestBlockHeight", queryLatestBlockHeight)

	log.Info("Starting server...")
	router.Run(":" + *port)

    // exit



}
