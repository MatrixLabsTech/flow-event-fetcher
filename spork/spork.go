/**
 * spork/spork.go
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
package spork

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk/client"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Spork struct {
	Name       string `json:"name"`
	RootHeight uint64 `json:"rootHeight"`
	AccessNode string `json:"accessNode"`
}

func ReadJSONFromUrl(url string) ([]Spork, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	var sporkList []Spork
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	respByte := buf.Bytes()
	if err := json.Unmarshal(respByte, &sporkList); err != nil {
		return nil, err
	}

	return sporkList, nil
}

type ResolvedAccessNodeList struct {
	Start      uint64
	End        uint64
	AccessNode string
}

type SporkStore struct {
	sync.Mutex

	SporkList []Spork

	url string

	readClient *client.Client
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

func (e *EventResult) JSON() interface{} {
	result := make([]interface{}, 0)

	for _, blockEvent := range e.BlockEvents {
		if len(blockEvent.Events) > 0 {
			for _, event := range blockEvent.Events {
				result = append(result, map[string]interface{}{
					"blockId":       blockEvent.Height,
					"index":         event.EventIndex,
					"type":          event.Type,
					"eventId":       event.ID(),
					"transactionId": event.TransactionID.String(),
					"values":        eventToJSON(&(event.Value))})
			}
		}
	}

	return result
}

func New(url string) *SporkStore {
	ss := &SporkStore{url: url}
	err := ss.SyncSpork()
	if err != nil {
		panic(err)
	}
	err = ss.newReadClient()
	if err != nil {
		panic(err)
	}

	return ss
}

func (ss *SporkStore) SyncSpork() error {
	ss.Lock()
	defer ss.Unlock()
	var err error = nil
	ss.SporkList, err = ReadJSONFromUrl(ss.url)
	log.Info("sync", ss.SporkList)
	return err
}

func (ss *SporkStore) resolveAccessNodes(start uint64, end uint64) ([]ResolvedAccessNodeList, error) {
	if end-start > 2000 {
		return nil, errors.New("cannot query more than 2000 blocks")
	}

	result := make([]ResolvedAccessNodeList, 0)

	startNodeIdx, err := ss.locateNode(start)
	if err != nil {
		return nil, err
	}

	endNodeIdx, err := ss.locateNode(end)
	if err != nil {
		return nil, err
	}

	if startNodeIdx == endNodeIdx {
		result = append(result, ResolvedAccessNodeList{Start: start, End: end, AccessNode: ss.SporkList[startNodeIdx].AccessNode})
	} else {
		result = append(result, ResolvedAccessNodeList{Start: start, End: ss.SporkList[endNodeIdx].RootHeight, AccessNode: ss.SporkList[startNodeIdx].AccessNode})
		result = append(result, ResolvedAccessNodeList{Start: ss.SporkList[endNodeIdx].RootHeight, End: end, AccessNode: ss.SporkList[endNodeIdx].AccessNode})
	}

	return result, nil
}

func (ss *SporkStore) locateNode(index uint64) (int, error) {
	left := 0
	right := len(ss.SporkList) - 1
	var mid int = 0
	var ret int = 0
	for left < right-1 {
		mid = (left + (right-left)/2)
		if ss.SporkList[mid].RootHeight > index {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if index < ss.SporkList[left].RootHeight {
		ret = left - 1
	} else if index < ss.SporkList[right].RootHeight {
		ret = right - 1
	} else {
		ret = right
	}
	if ret < 0 {
		return 0, errors.New("index out of earlist supported block")
	}
	return ret, nil
}

func (ss *SporkStore) newReadClient() error {
	flowClient, err := client.New(ss.SporkList[len(ss.SporkList)-1].AccessNode, grpc.WithInsecure(), grpc.WithMaxMsgSize(40e6))
	if err != nil {
		return err
	}
	ss.readClient = flowClient
	return nil
}

func (ss *SporkStore) checkReaderHealthy() error {
	ctx := context.Background()
	err := ss.readClient.Ping(ctx)
	if err != nil {
		return ss.newReadClient()
	}
	return nil
}

func (ss *SporkStore) QueryLatestBlockHeight() (uint64, error) {
	ss.Lock()
	defer ss.Unlock()
	ss.checkReaderHealthy()
	ctx := context.Background()
	header, err := ss.readClient.GetLatestBlockHeader(ctx, true)
	return header.Height, err
}

func (ss *SporkStore) QueryEventByBlockRange(event string, start uint64, end uint64) ([]client.BlockEvents, error) {

	ctx := context.Background()

	ret := make([]client.BlockEvents, 0)

	resolvedAccessNodeList, err := ss.resolveAccessNodes(uint64(start), uint64(end))
	if err != nil {
		return nil, err
	}

	for _, node := range resolvedAccessNodeList {
		fmt.Println(node)

		flowClient, err := client.New(node.AccessNode, grpc.WithInsecure(), grpc.WithMaxMsgSize(40e6))
		defer flowClient.Close()
		defer log.Info("close client from:", node.AccessNode)

		if err != nil {
			return nil, err
		}

		for i := uint64(node.Start); i < node.End; i += 200 {
			results, err := flowClient.GetEventsForHeightRange(ctx, client.EventRangeQuery{
				Type:        event,
				StartHeight: i,
				EndHeight:   uint64(math.Min(float64(node.End), float64(i+200))),
			})

			if err != nil {
				return nil, err
			}
			ret = append(ret, results...)
		}

	}
	return ret, nil
}

func EventToJSON(e *cadence.Event) interface{} {
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

func BlockEventsToJSON(e []client.BlockEvents) []interface{} {
	result := make([]interface{}, 0)

	for _, blockEvent := range e {
		if len(blockEvent.Events) > 0 {
			for _, event := range blockEvent.Events {
				result = append(result, map[string]interface{}{
					"blockId":       blockEvent.Height,
					"index":         event.EventIndex,
					"type":          event.Type,
					"eventId":       event.ID(),
					"transactionId": event.TransactionID.String(),
					"values":        eventToJSON(&(event.Value))})
			}
		}
	}

	return result
}
