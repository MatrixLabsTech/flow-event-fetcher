package main

import (
	"fmt"

	"github.com/Lucklyric/ultimate-flow-event-fetcher/spork"
)

func main() {
	sporkJsonUrl := "https://raw.githubusercontent.com/Lucklyric/flow-spork-info/main/spork.json"
	sporkStore := spork.New(sporkJsonUrl)
	fmt.Println("sporkJsonUrl:", sporkJsonUrl)

	event := "A.1654653399040a61.FlowToken.TokensDeposited"


    // store will automatically fetch events
    // {19050753 19051853 access.mainnet.nodes.onflow.org:9000}
    // with batchSize 200 blocks
	ret, err := sporkStore.QueryEventByBlockRange(event, 13405050, 13405100)
	if err != nil {
		panic(err)
	}
	jsonRet := spork.BlockEventsToJSON(ret)
	fmt.Println(jsonRet)
	fmt.Println("Total fetched events:", len(jsonRet))

    // store will automatically fetch events with
    // {19049753 19050753 access-001.mainnet13.nodes.onflow.org:9000}
    // {19050753 19051484 access.mainnet.nodes.onflow.org:9000}
	ret, err = sporkStore.QueryEventByBlockRange(event, 19049753, 19051484)
	if err != nil {
		panic(err)
	}
	jsonRet = spork.BlockEventsToJSON(ret)
	fmt.Println(jsonRet)
	fmt.Println("Total fetched events:", len(jsonRet))

    // store will automatically fetch events with
    // {11905073 19051853 access.mainnet.nodes.onflow.org:9000}
	ret, err = sporkStore.QueryEventByBlockRange(event, 19050753, 19051853)
	if err != nil {
		panic(err)
	}
	jsonRet = spork.BlockEventsToJSON(ret)
	fmt.Println(jsonRet)
	fmt.Println("Total fetched events:", len(jsonRet))
}
