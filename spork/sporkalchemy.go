package spork

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/onflow/flow-go-sdk/client"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "github.com/MatrixLabsTech/flow-event-fetcher/proto/v1"
)

type Alchemy struct {
	sync.Mutex

	endPoint string

	apiContext context.Context

	flowClient *client.Client

	maxQueryBlocks uint64

	queryBatchSize uint64
}

// init initializes the spork alchemy
func (alchemy *Alchemy) init() error {
	alchemy.Lock()
	defer alchemy.Unlock()

	if alchemy.flowClient == nil {
		err := alchemy.newClient()
		if err != nil {
			return err
		}
	}

	return nil
}

func NewSporkAlchemy(endPoint string, apiKey string, maxQueryBlocks uint64, queryBatchSize uint64) *Alchemy {
	ss := &Alchemy{
		endPoint:       endPoint,
		maxQueryBlocks: maxQueryBlocks,
		queryBatchSize: queryBatchSize,
	}

	header := metadata.New(map[string]string{
		"api_key": apiKey,
	})

	ss.apiContext = metadata.NewOutgoingContext(context.Background(), header)

	// init the spork alchemy
	ss.init()

	return ss
}

func (alchemy *Alchemy) checkClientHealthy() error {
	err := alchemy.flowClient.Ping(alchemy.apiContext)
	if err != nil {
		// close previous client
		alchemy.flowClient.Close()

		// create new client
		log.Error("SporkAlchemy: flow client is not healthy with error ", err)
		log.Info("SporkAlchemy: trying to reinitialize flow client")
		return alchemy.newClient()
	}
	return nil
}

func (alchemy *Alchemy) newClient() error {
	log.Info("SporkAlchemy: initializing flow client")
	log.Info(alchemy.endPoint)

	flowClient, err := client.New(alchemy.endPoint, grpc.WithInsecure(), grpc.WithMaxMsgSize(40e6))
	if err != nil {
		log.Error("SporkAlchemy: failed to initialize flow client", "err", err)
		return err
	}
	alchemy.flowClient = flowClient
	return nil
}

// QueryLatestBlockHeight queries the latest block height
func (alchemy *Alchemy) QueryLatestBlockHeight(_ context.Context) (uint64, error) {
	// thread safe
	alchemy.Lock()
	defer alchemy.Unlock()

	err := alchemy.checkClientHealthy()
	if err != nil {
		return 0, err
	}

	block, err := alchemy.flowClient.GetLatestBlock(context.Background(), true)
	if err != nil {
		return 0, err
	}

	return block.Height, nil
}

// QueryAllEventByBlockRange returns all events in a block range
func (alchemy *Alchemy) QueryAllEventByBlockRange(_ context.Context, _ uint64, _ uint64) ([]client.BlockEvents,
	[]*pb.QueryAllEventByBlockRangeResponseErrorTransaction, error) {
	return nil, nil, errors.New("not implemented")
}

// QueryEventByBlockRange returns events in a block range
func (alchemy *Alchemy) QueryEventByBlockRange(_ context.Context, event string, start uint64, end uint64) ([]client.BlockEvents, error) {
	// thread safe
	alchemy.Lock()
	defer alchemy.Unlock()

	err := alchemy.checkClientHealthy()
	if err != nil {
		return nil, err
	}

	events := make([]client.BlockEvents, 0)

	tmpQueryBatchSize := alchemy.queryBatchSize

	ret, err := IterQueryEventByBlockRange(alchemy.apiContext, alchemy.flowClient, event, start, end, tmpQueryBatchSize)
	if err != nil {
		return nil, err
	}
	events = append(events, ret...)

	return events, nil
}

// SyncSpork with not implementation log
func (alchemy *Alchemy) SyncSpork() error {
	return nil
}

// String method return the string representation of the spork alchemy
func (alchemy *Alchemy) String() string {
	// all basic information
	return fmt.Sprintf("SporkAlchemy: {endPoint: %s, maxQueryBlocks: %d, queryBatchSize: %d}", alchemy.endPoint, alchemy.maxQueryBlocks, alchemy.queryBatchSize)
}

// Close the spork alchemy
func (alchemy *Alchemy) Close() error {
	if alchemy.flowClient != nil {
		err := alchemy.flowClient.Close()
		log.Info("SporkAlchemy: flow client closed")
		return err
	}
	return nil
}
