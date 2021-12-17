package spork

import (
	"context"
	"fmt"
	"sync"

	"github.com/onflow/flow-go-sdk/client"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type SporkAlchemy struct {
	sync.Mutex

	endPoint string

	apiContext context.Context

	flowClient *client.Client

	maxQueryBlocks uint64

	queryBatchSize uint64
}

// init initializes the spork alchemy
func (alchemy *SporkAlchemy) init() error {
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

func NewSporkAlchemy(endPoint string, apiKey string, maxQueryBlocks uint64, queryBatchSize uint64) *SporkAlchemy {
	ss := &SporkAlchemy{
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

func (alchemy *SporkAlchemy) checkClientHealthy() error {
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

func (alchemy *SporkAlchemy) newClient() error {
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

// QueryLatestBlockHeight
func (alchemy *SporkAlchemy) QueryLatestBlockHeight() (uint64, error) {
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

// queryEventByBlockRange
func (alchemy *SporkAlchemy) QueryEventByBlockRange(event string, start uint64, end uint64) ([]client.BlockEvents, error) {
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
func (alchemy *SporkAlchemy) SyncSpork() error {
	return nil
}

// String method return the string representation of the spork alchemy
func (alchemy *SporkAlchemy) String() string {
	// all basic information
	return fmt.Sprintf("SporkAlchemy: {endPoint: %s, maxQueryBlocks: %d, queryBatchSize: %d}", alchemy.endPoint, alchemy.maxQueryBlocks, alchemy.queryBatchSize)
}
