/**
 * spork/sporkstore.go
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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/client"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	pb "github.com/MatrixLabsTech/flow-event-fetcher/proto/v1"
)

var (
	NetworkConfigURL = "https://raw.githubusercontent.com/onflow/flow/master/sporks.json"
	TestnetEndpoints = "access.devnet.nodes.onflow.org:9000"
)

type Spork struct {
	ID         float64 `json:"-"`
	Name       string  `json:"name"`
	RootHeight uint64  `json:"rootHeight"`
	AccessNode string  `json:"accessNode"`
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

type FlowNetworkConfig struct {
	Networks map[string]map[string]StageNetworkConfig `json:"networks"`
}

type StageNetworkConfig struct {
	ID          float64     `json:"id"`
	Name        string      `json:"name"`
	RootHeight  json.Number `json:"rootHeight"`
	AccessNodes []string    `json:"accessNodes"`
}

// ReadFlowNetworkConfigFromUrl reads the flow network config from the given url.
func ReadFlowNetworkConfigFromUrl(stage string) ([]Spork, error) {
	resp, err := http.Get(NetworkConfigURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	networks := FlowNetworkConfig{}
	err = json.NewDecoder(resp.Body).Decode(&networks)
	if err != nil {
		return nil, err
	}
	if networks.Networks == nil {
		return nil, errors.New("no networks found")
	}
	cfg, ok := networks.Networks[stage]
	if !ok {
		return nil, fmt.Errorf("no network found for stage %s", stage)
	}
	sporkList := make([]Spork, 0, len(cfg))
	for _, c := range cfg {
		rootHeight, err := c.RootHeight.Int64()
		if err != nil {
			return nil, err
		}
		var accessNode string
		if len(c.AccessNodes) > 0 {
			accessNode = c.AccessNodes[0]
		}
		if stage == "testnet" && accessNode == "" {
			accessNode = TestnetEndpoints
		}
		sporkList = append(sporkList, Spork{
			ID:         c.ID,
			Name:       c.Name,
			RootHeight: uint64(rootHeight),
			AccessNode: accessNode,
		})
	}
	sort.Slice(sporkList, func(i, j int) bool {
		return sporkList[i].ID < sporkList[j].ID
	})
	return sporkList, nil
}

type Store struct {
	SporkList      []Spork
	stage          string
	clients        sync.Map // client set in NewSporkStore
	maxQueryBlocks uint64
	queryBatchSize uint64
}

func NewSporkStore(stage string, maxQueryBlocks uint64, queryBatchSize uint64) *Store {
	ss := &Store{stage: stage, maxQueryBlocks: maxQueryBlocks, queryBatchSize: queryBatchSize}
	err := ss.SyncSpork()
	if err != nil {
		panic(err)
	}
	return ss
}

func (ss *Store) getLatestClient() (*client.Client, error) {
	node, err := ss.latestAccessNode()
	if err != nil {
		return nil, err
	}
	return ss.getClient(node)
}

func (ss *Store) String() string {
	// with basic information with sporkList
	return fmt.Sprintf("SporkStore{stage: %s, maxQueryBlocks: %d, queryBatchSize: %d, sporkList: %v}\n", ss.stage, ss.maxQueryBlocks, ss.queryBatchSize, ss.SporkList)
}

func (ss *Store) setClient(r *ResolvedAccessNodeList) (*client.Client, error) {
	c, err := ss.newClient(r.AccessNode)
	if err != nil {
		return nil, err
	}
	ss.clients.Store(r.Index, c)
	return c, nil
}

func (ss *Store) getClient(r *ResolvedAccessNodeList) (*client.Client, error) {
	v, ok := ss.clients.Load(r.Index)
	if !ok {
		return ss.setClient(r)
	}
	c := v.(*client.Client)
	ctx := context.Background()
	log.Info("Start to ping")

	if err := c.Ping(ctx); err != nil {
		//If an error has been reported at this time, the connection has been disconnected and needs to be reconnected.
		return ss.setClient(r)
	}
	return c, nil
}

func (ss *Store) newClient(endpoint string) (*client.Client, error) {
	return client.New(endpoint, grpc.WithInsecure(), grpc.WithMaxMsgSize(40e6))
}

func (ss *Store) SyncSpork() error {
	var err error
	ss.SporkList, err = ReadFlowNetworkConfigFromUrl(ss.stage)
	log.Info("sync", ss.SporkList)
	return err
}

func (ss *Store) latestAccessNode() (*ResolvedAccessNodeList, error) {
	if len(ss.SporkList) == 0 {
		return nil, errors.New("no spork available")
	}
	return &ResolvedAccessNodeList{
		Index:      len(ss.SporkList) - 1,
		AccessNode: ss.SporkList[len(ss.SporkList)-1].AccessNode,
	}, nil
}

func (ss *Store) resolveAccessNodes(start uint64, end uint64) ([]ResolvedAccessNodeList, error) {
	if end-start > ss.maxQueryBlocks {
		return nil, errors.New("total blocks is greater than maxQueryBlocks")
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
		result = append(result, ResolvedAccessNodeList{
			Index:      startNodeIdx,
			Start:      start,
			End:        end,
			AccessNode: ss.SporkList[startNodeIdx].AccessNode})
	} else {
		result = append(result,
			ResolvedAccessNodeList{
				Index:      startNodeIdx,
				Start:      start,
				End:        ss.SporkList[endNodeIdx].RootHeight - 1,
				AccessNode: ss.SporkList[startNodeIdx].AccessNode},
			ResolvedAccessNodeList{
				Index:      endNodeIdx,
				Start:      ss.SporkList[endNodeIdx].RootHeight,
				End:        end,
				AccessNode: ss.SporkList[endNodeIdx].AccessNode})
	}

	return result, nil
}

func (ss *Store) locateNode(index uint64) (int, error) {
	left := 0
	right := len(ss.SporkList) - 1
	var mid, ret int
	for left < right-1 {
		mid = left + (right-left)/2
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

func (ss *Store) QueryLatestBlockHeight(ctx context.Context) (uint64, error) {
	c, err := ss.getLatestClient()
	if err != nil {
		return 0, err
	}
	header, err := c.GetLatestBlockHeader(ctx, true)
	if err != nil {
		return 0, err
	}
	return header.Height, err
}

func (ss *Store) QueryEventByBlockRange(ctx context.Context, event string, start uint64, end uint64) ([]client.BlockEvents, error) {
	events := make([]client.BlockEvents, 0)

	resolvedAccessNodeList, err := ss.resolveAccessNodes(start, end)
	if err != nil {
		return nil, err
	}

	tmpQueryBatchSize := ss.queryBatchSize
	for _, node := range resolvedAccessNodeList {
		flowClient, err := ss.getClient(&node)

		if err != nil {
			return nil, err
		}

		ret, err := IterQueryEventByBlockRange(ctx, flowClient, event, node.Start, node.End, tmpQueryBatchSize)
		if err != nil {
			return nil, err
		}
		events = append(events, ret...)

	}
	return events, nil
}

func (ss *Store) QueryAllEventByBlockRange(ctx context.Context, start, end uint64) (events []client.BlockEvents,
	errTransactions []*pb.QueryAllEventByBlockRangeResponseErrorTransaction, err error) {
	events = make([]client.BlockEvents, 0)
	errTransactions = make([]*pb.QueryAllEventByBlockRangeResponseErrorTransaction, 0)

	resolvedAccessNodeList, err := ss.resolveAccessNodes(start, end)
	if err != nil {
		return nil, nil, err
	}
	blockEventsChan := make(chan client.BlockEvents, end-start+1)
	errChan := make(chan error, end-start+1)
	errTransactionChan := make(chan []*pb.QueryAllEventByBlockRangeResponseErrorTransaction, end-start+1)
	var wg sync.WaitGroup
	wg.Add(int(end - start + 1))
	for i := range resolvedAccessNodeList {
		flowClient, err := ss.getClient(&resolvedAccessNodeList[i])
		if err != nil {
			return nil, nil, err
		}
		for height := resolvedAccessNodeList[i].Start; height <= resolvedAccessNodeList[i].End; height++ {
			go ss.getEventByBlockHeight(&wg, ctx, flowClient, height, blockEventsChan, errTransactionChan, errChan)
		}
	}
	wg.Wait()
	close(blockEventsChan)
	close(errChan)
	close(errTransactionChan)

	if len(errChan) > 0 {
		return nil, nil, <-errChan
	}
	for e := range blockEventsChan {
		events = append(events, e)
	}

	if len(errTransactionChan) > 0 {
		for e := range errTransactionChan {
			if len(e) > 0 {
				errTransactions = append(errTransactions, e...)
			}
		}
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].Height < events[j].Height
	})

	for n := range events {
		sort.Slice(events[n].Events, func(i, j int) bool {
			if events[n].Events[i].TransactionIndex != events[n].Events[j].TransactionIndex {
				return events[n].Events[i].TransactionIndex < events[n].Events[j].TransactionIndex
			}
			return events[n].Events[i].EventIndex < events[n].Events[j].EventIndex
		})
	}

	return
}

func (ss *Store) getEventByBlockHeight(
	wg *sync.WaitGroup,
	ctx context.Context,
	flowClient *client.Client,
	height uint64,
	blockEventChan chan client.BlockEvents,
	errTransactionChan chan []*pb.QueryAllEventByBlockRangeResponseErrorTransaction,
	errChan chan error) {

	defer wg.Done()
	block, err := flowClient.GetBlockByHeight(ctx, height)
	if err != nil {
		log.Errorf("get block by height error: %s", err.Error())
		errChan <- err
		return
	}
	event := client.BlockEvents{
		BlockID:        block.ID,
		Height:         block.Height,
		BlockTimestamp: block.Timestamp,
		Events:         make([]flow.Event, 0),
	}
	eventChan := make(chan []flow.Event, len(block.BlockPayload.CollectionGuarantees))
	errTransactionCollectionChan := make(chan []*pb.QueryAllEventByBlockRangeResponseErrorTransaction,
		len(block.BlockPayload.CollectionGuarantees))
	var wgCollection sync.WaitGroup
	wgCollection.Add(len(block.BlockPayload.CollectionGuarantees))
	for j := range block.BlockPayload.CollectionGuarantees {
		go ss.getEventByCollectionID(&wgCollection,
			ctx, flowClient,
			block.BlockPayload.CollectionGuarantees[j].CollectionID,
			eventChan, errTransactionCollectionChan, errChan)
	}
	wgCollection.Wait()
	close(eventChan)
	close(errTransactionCollectionChan)
	if len(errChan) > 0 {
		return
	}
	errTransactionCollection := make([]*pb.QueryAllEventByBlockRangeResponseErrorTransaction, 0)
	if len(errTransactionCollectionChan) > 0 {
		for e := range errTransactionCollectionChan {
			if len(e) > 0 {
				errTransactionCollection = append(errTransactionCollection, e...)
			}
		}
	}
	for e := range eventChan {
		event.Events = append(event.Events, e...)
	}
	if len(errTransactionCollection) > 0 {
		errTransactionChan <- errTransactionCollection
	}

	blockEventChan <- event
}

func (ss *Store) getEventByCollectionID(
	wg *sync.WaitGroup,
	ctx context.Context,
	flowClient *client.Client,
	id flow.Identifier,
	eventChan chan []flow.Event,
	errTransactionsChan chan []*pb.QueryAllEventByBlockRangeResponseErrorTransaction,
	errChan chan error) {

	defer wg.Done()
	collection, err := flowClient.GetCollection(ctx, id)
	if err != nil {
		log.Errorf("get collection by id error: %s", err.Error())
		errChan <- err
		return
	}

	eventTransactionChan := make(chan []flow.Event, len(collection.TransactionIDs))
	errTransactionChan := make(chan *pb.QueryAllEventByBlockRangeResponseErrorTransaction,
		len(collection.TransactionIDs))

	var wgTransaction sync.WaitGroup
	wgTransaction.Add(len(collection.TransactionIDs))
	for i := range collection.TransactionIDs {
		go ss.getEventByTransactionID(&wgTransaction, ctx,
			flowClient,
			collection.TransactionIDs[i],
			eventTransactionChan, errTransactionChan, errChan)
	}
	wgTransaction.Wait()
	close(eventTransactionChan)
	close(errTransactionChan)
	events := make([]flow.Event, 0)
	for e := range eventTransactionChan {
		events = append(events, e...)
	}
	errTransactions := make([]*pb.QueryAllEventByBlockRangeResponseErrorTransaction, 0)
	if len(errTransactionChan) > 0 {
		for e := range errTransactionChan {
			if e != nil {
				errTransactions = append(errTransactions, e)
			}
		}
	}
	if len(errTransactions) > 0 {
		errTransactionsChan <- errTransactions
	}
	eventChan <- events
}

func (ss *Store) getEventByTransactionID(wg *sync.WaitGroup,
	ctx context.Context,
	flowClient *client.Client,
	id flow.Identifier,
	eventChan chan []flow.Event,
	errTransactionChan chan *pb.QueryAllEventByBlockRangeResponseErrorTransaction,
	errChan chan error) {

	defer wg.Done()
	transaction, err := flowClient.GetTransactionResult(ctx, id)
	if err != nil {
		log.Errorf("get transaction by id error: %s", err.Error())
		errChan <- err
		return
	}
	if transaction.Error != nil {
		errTransactionChan <- &pb.QueryAllEventByBlockRangeResponseErrorTransaction{
			Error:         transaction.Error.Error(),
			TransactionId: id.String(),
		}
		return
	}
	eventChan <- transaction.Events
}
