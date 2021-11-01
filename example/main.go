/**
 * example/main.go
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

    "github.com/MatrixLabTech/flow-event-fetcher/spork"
)

func main() {
    sporkJsonUrl := "https://raw.githubusercontent.com/Lucklyric/flow-spork-info/main/spork.json"
    maxQueryCount := 2000
    batchSize := 5
    sporkStore := spork.New(sporkJsonUrl, uint64(maxQueryCount), uint64(batchSize))
    // display sprokStore above setup

    event := "A.1654653399040a61.FlowToken.TokensDeposited"


    // store will automatically fetch events
    // {19050753 19051853 access.mainnet.nodes.onflow.org:9000}
    ret, err := sporkStore.QueryEventByBlockRange(event, 13405050, 13405100)
    if err != nil {
        panic(err)
    }
    fmt.Println("Total fetched blocks:", len(ret))
    jsonRet := spork.BlockEventsToJSON(ret)
    fmt.Println("Total fetched events:", len(jsonRet))

    ret, err = sporkStore.QueryEventByBlockRange(event, 13405050, 13406060)
    if err != nil {
        panic(err)
    }
    fmt.Println("Total fetched blocks:", len(ret))
    jsonRet = spork.BlockEventsToJSON(ret)
    fmt.Println("Total fetched events:", len(jsonRet))
}
