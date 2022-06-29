/**
 * example/main.go
 * Copyright (c) 2021 Alvin(Xinyao) Sun <asun@matrixworld.io>
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
	"context"
	"fmt"

	"github.com/MatrixLabsTech/flow-event-fetcher/spork"

	pb "github.com/MatrixLabsTech/flow-event-fetcher/proto/v1"
)

func main() {
	sporkJsonUrl := "https://raw.githubusercontent.com/Lucklyric/flow-spork-info/main/spork.json"
	sporkStore := spork.NewSporkStore(sporkJsonUrl, 2000, 200)
	fmt.Println("sporkJsonUrl:", sporkJsonUrl)

	event := "A.1654653399040a61.FlowToken.TokensDeposited"

	// store will automatically fetch events
	// {19050753 19051853 access.mainnet.nodes.onflow.org:9000}
	// with batchSize 200 blocks
	ret, err := sporkStore.QueryEventByBlockRange(context.Background(), event, 13405050, 13405100)
	if err != nil {
		panic(err)
	}
	jsonRet := spork.BlockEventsToJSON(ret)
	fmt.Println(jsonRet)
	fmt.Println("Total fetched events:", len(jsonRet))

	// store will automatically fetch events with
	// {19049753 19050753 access-001.mainnet13.nodes.onflow.org:9000}
	// {19050753 19051484 access.mainnet.nodes.onflow.org:9000}
	ret, err = sporkStore.QueryEventByBlockRange(context.Background(), event, 19049753, 19051484)
	if err != nil {
		panic(err)
	}
	jsonRet = spork.BlockEventsToJSON(ret)
	fmt.Println(jsonRet)
	fmt.Println("Total fetched events:", len(jsonRet))

	// store will automatically fetch events with
	// {11905073 19051853 access.mainnet.nodes.onflow.org:9000}
	ret, err = sporkStore.QueryEventByBlockRange(context.Background(), event, 19050753, 19051853)
	if err != nil {
		panic(err)
	}
	jsonRet = spork.BlockEventsToJSON(ret)
	fmt.Println(jsonRet)
	fmt.Println("Total fetched events:", len(jsonRet))

	// store will automatically fetch events with
	// {11905073 19051853 access.mainnet.nodes.onflow.org:9000}
	errorTransactions := make([]*pb.QueryAllEventByBlockRangeResponseErrorTransaction, 0)
	ret, errorTransactions, err = sporkStore.QueryAllEventByBlockRange(context.Background(), 19050753, 19051853)
	if err != nil {
		panic(err)
	}
	jsonRet = spork.BlockEventsToJSON(ret)
	fmt.Println(jsonRet)
	fmt.Println("Total fetched events:", len(jsonRet))
	fmt.Println(errorTransactions)
}
