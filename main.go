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

//go:generate swag init

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	pb "github.com/MatrixLabsTech/flow-event-fetcher/proto/v1"
	"github.com/MatrixLabsTech/flow-event-fetcher/spork"
)

var flowClient spork.FlowClient

var backendMode = "alchemy"

type ResponseError struct {
	Error string `json:"error"`
}

// Version get version
// @Summary get version
// @Description get version
// @Tags flow-event-fetcher
// @Accept  application/json
// @Product application/json
// @Success 200 {object} pb.VersionResponse
// @Router /version [get]
func version(c *gin.Context) {
	c.JSON(http.StatusOK, pb.VersionResponse{
		Version:     "1.0.0",
		BackendMode: backendMode,
	})
}

// SyncSpork sync spork
// @Summary sync spork
// @Description sync spork
// @Tags flow-event-fetcher
// @Accept  application/json
// @Product application/json
// @Success 200 {object} pb.QueryLatestBlockHeightResponse
// @Failure 500 {object} ResponseError
// @Router /syncSpork [get]
func syncSpork(c *gin.Context) {
	err := flowClient.SyncSpork()
	if err != nil {
		log.Error(err.Error())
		c.JSON(http.StatusInternalServerError, ResponseError{Error: err.Error()})
		return
	}
	c.JSON(http.StatusOK, pb.SyncSporkResponse{Spork: flowClient.String()})
}

// queryLatestBlockHeight query the latest block height
// @Summary queries the latest block height
// @Description queries the latest block height
// @Tags flow-event-fetcher
// @Accept  application/json
// @Product application/json
// @Success 200 {object} pb.QueryLatestBlockHeightResponse
// @Failure 500 {object} ResponseError
// @Router /queryLatestBlockHeight [get]
func queryLatestBlockHeight(c *gin.Context) {
	height, err := flowClient.QueryLatestBlockHeight()
	if err != nil {
		log.Error(err.Error())
		c.JSON(http.StatusInternalServerError, ResponseError{Error: err.Error()})
		return
	}
	c.JSON(http.StatusOK, pb.QueryLatestBlockHeightResponse{LatestBlockHeight: height})
}

// queryEventByBlockRange query event by block range
// @Summary queries event by block range
// @Description queries event by block range
// @Tags flow-event-fetcher
// @Accept  application/json
// @Product application/json
// @Param data body pb.QueryEventByBlockRangeRequest true "data"
// @Success 200 {object} []pb.QueryEventByBlockRangeResponseEvent
// @Failure 500 {object} ResponseError
// @Router /queryEventByBlockRange [post]
func queryEventByBlockRange(c *gin.Context) {
	//var queryEventByBlockRangeDto QueryEventByBlockRangeDto
	var queryEventByBlockRangeDto pb.QueryEventByBlockRangeRequest
	err := c.Bind(&queryEventByBlockRangeDto)
	if err != nil {
		log.Error(err.Error())
		c.JSON(http.StatusBadRequest, ResponseError{Error: err.Error()})
		return
	}
	log.Info(fmt.Sprintf("query %s, from %d to %d",
		queryEventByBlockRangeDto.Event,
		queryEventByBlockRangeDto.Start,
		queryEventByBlockRangeDto.End))

	ret, err := flowClient.QueryEventByBlockRange(
		queryEventByBlockRangeDto.Event,
		queryEventByBlockRangeDto.Start,
		queryEventByBlockRangeDto.End)
	if err != nil {
		log.Error(err.Error())
		c.JSON(http.StatusBadRequest, ResponseError{Error: err.Error()})
		return
	}

	jsonRet := spork.BlockEventsToJSON(ret)
	log.Info(fmt.Sprintf("Got %d events", len(jsonRet)))
	c.JSON(http.StatusOK, jsonRet)

}

// @title flow-event-fetcher API
// @version 1.0.1
// @description flow-event-fetcher interface documentation
// @host localhost:8989
// @BasePath
func main() {
	port := flag.String("port", "8989", "port to listen on")
	stage := flag.String("stage", "testnet", "network stage")
	alchemyEndpoint := flag.String("alchemyEndpoint", "", "alchemy endpoint")
	alchemyApiKey := flag.String("alchemyApiKey", "", "alchemy api key")
	useAlchemy := flag.Bool("useAlchemy", true, "use alchemy")
	maxQueryBlocks := flag.Uint64("maxQueryBlocks", 2000, "max query blocks")
	queryBatchSize := flag.Uint64("queryBatchSize", 200, "query batch size")
	flag.Parse()

	// check if useAlchemy
	if *useAlchemy {
		if *alchemyEndpoint == "" {
			log.Fatal("alchemy endpoint is required")
		}
		if *alchemyApiKey == "" {
			log.Fatal("alchemy api key is required")
		}
		flowClient = spork.NewSporkAlchemy(*alchemyEndpoint, *alchemyApiKey, *maxQueryBlocks, *queryBatchSize)

	} else {
		flowClient = spork.NewSporkStore(*stage, *maxQueryBlocks, *queryBatchSize)
	}

	// display formatted sporkStore configuration
	log.Info(fmt.Sprintf("sporkStore configuration: %s", flowClient.String()))
	router := gin.Default()

	router.Use(gin.LoggerWithWriter(os.Stderr))

	router.GET("/version", version)
	router.GET("/syncSpork", syncSpork)
	router.POST("/queryEventByBlockRange", queryEventByBlockRange)
	router.GET("/queryLatestBlockHeight", queryLatestBlockHeight)

	log.Info("Starting server...")
	router.Run(":" + *port)
}
