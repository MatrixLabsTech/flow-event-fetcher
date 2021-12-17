/**
 * spork/spork.go
 * Copyright (c) 2021 Alvin(Xinyao) Sun <asun@matrixworld.org>
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
	"context"

	"github.com/onflow/cadence"
	"github.com/onflow/flow-go-sdk/client"
	log "github.com/sirupsen/logrus"
)

type FlowClient interface {
	String() string
	QueryEventByBlockRange(event string, start uint64, end uint64) ([]client.BlockEvents, error)
	QueryLatestBlockHeight() (uint64, error)
	SyncSpork() error
    Close() error
}

type ResolvedAccessNodeList struct {
	Start      uint64
	End        uint64
	AccessNode string
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

func EventToJSON(e *cadence.Event) []map[string]interface{} {
	preparedFields := make([]map[string]interface{}, 0)
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

func BlockEventsToJSON(e []client.BlockEvents) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)

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

func IterQueryEventByBlockRange(ctx context.Context, ss *client.Client, event string, start uint64, end uint64, defaultBatchSize uint64) ([]client.BlockEvents, error) {
	events := make([]client.BlockEvents, 0)
	tmpQueryBatchSize := defaultBatchSize
	for i := start; i <= end; i += tmpQueryBatchSize {
		// reset tmpQueryBatchSize to default
		tmpQueryBatchSize = defaultBatchSize

		// get the block range
		startBlock := i

		for {
			endBlock := i + tmpQueryBatchSize - 1
			if endBlock > end {
				endBlock = end
			}

			log.Info("query block range: ", startBlock, " - ", endBlock)
			results, err := ss.GetEventsForHeightRange(ctx, client.EventRangeQuery{
				Type:        event,
				StartHeight: startBlock,
				EndHeight:   endBlock,
			})

			if err != nil {
				// log error with start and end
				log.Error("FlowClient: failed to get events for height range", "start", startBlock, "end", endBlock, "err", err)

				// return error if tmpQueryBatchSize = 1
				if tmpQueryBatchSize == 1 {
					return nil, err
				}

				// decrease tmpQueryBatchSize by half
				tmpQueryBatchSize = tmpQueryBatchSize / 2
				log.Info("SporkAlchemy: decrease query batch size to ", tmpQueryBatchSize)

				continue
			}
			events = append(events, results...)
			break
		}
	}

	return events, nil
}
