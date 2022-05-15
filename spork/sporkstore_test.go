package spork

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSporkStoreInit(t *testing.T) {
	store := NewSporkStore(
		"mainnet", 5000, 100)
	require.NotNil(t, store, "store should not be nil")

	err := store.checkReaderHealthy()
	require.Nil(t, err, "err should be nil for healthy reader")
}

func TestE2EFlowTransferEventFetchingCrossSpork(t *testing.T) {
	t.Log("TestE2EFlowTransferEventFetching")

	t.Log("TestE2EFlowTransferEventFetching: init")
	storeBatch200 := NewSporkStore(
		"mainnet", 2000, 200)
	require.NotNil(t, storeBatch200, "store should not be nil")

	storeBatch100 := NewSporkStore(
		"mainnet", 2000, 100)

	require.NotNil(t, storeBatch100, "store should not be nil")

	testEventSignature := "A.1654653399040a61.FlowToken.TokensDeposited"

	testEventStartBlock := 21291000

	testEventEndBlock := 21291000 + 2000

	t.Log("TestE2EFlowTransferEventFetching: fetching events")
	eventsFromBatch200, err := storeBatch200.QueryEventByBlockRange(testEventSignature, uint64(testEventStartBlock), uint64(testEventEndBlock))

	require.Nil(t, err, "err should be nil for storeBatch200 query")

	t.Log("TestE2EFlowTransferEventFetching: fetching events with batch 100 got ", len(eventsFromBatch200))

	eventsFromBatch100, err := storeBatch100.QueryEventByBlockRange(testEventSignature, uint64(testEventStartBlock), uint64(testEventEndBlock))

	require.Nil(t, err, "err should be nil for storeBatch100 query")

	t.Log("TestE2EFlowTransferEventFetching: fetching events with batch 100 got ", len(eventsFromBatch100))

	require.Equal(t, len(eventsFromBatch200), len(eventsFromBatch100), "eventsFromBatch200 and eventsFromBatch100 should be equal")

}

func TestE2EFlowTransferEventFetchingBatchConsistence(t *testing.T) {

	storeBatch1 := NewSporkStore(
		"mainnet", 2000, 1)

	require.NotNil(t, storeBatch1, "store should not be nil")

	storeBatch200 := NewSporkStore(
		"mainnet", 2000, 200)

	require.NotNil(t, storeBatch200, "store should not be nil")

	testEventSignature := "A.1654653399040a61.FlowToken.TokensDeposited"

	testEventStartBlock := 21291000

	testEventEndBlock := 21291000 + 20

	t.Log("TestE2EFlowTransferEventFetching: fetching events")

	eventsFromBatch1, err := storeBatch1.QueryEventByBlockRange(testEventSignature, uint64(testEventStartBlock), uint64(testEventEndBlock))

	require.Nil(t, err, "err should be nil for storeBatch1 query")

	t.Log("TestE2EFlowTransferEventFetching: fetching events with batch  1 got ", len(eventsFromBatch1))

	eventsFromBatch200, err := storeBatch200.QueryEventByBlockRange(testEventSignature, uint64(testEventStartBlock), uint64(testEventEndBlock))

	require.Nil(t, err, "err should be nil for storeBatch200 query")

	t.Log("TestE2EFlowTransferEventFetching: fetching events with batch 200 got ", len(eventsFromBatch200))

	require.Equal(t, len(eventsFromBatch1), len(eventsFromBatch200), "eventsFromBatch1 and eventsFromBatch200 should be equal")
}

func TestE2EFlowTransferAllEventFetchingBatchConsistence(t *testing.T) {

	storeBatch1 := NewSporkStore(
		"mainnet", 2000, 1)
	require.NotNil(t, storeBatch1, "store should not be nil")

	storeBatch200 := NewSporkStore(
		"mainnet", 2000, 200)

	require.NotNil(t, storeBatch200, "store should not be nil")

	testEventStartBlock := 21291000

	testEventEndBlock := 21291000 + 20

	t.Log("TestE2EFlowTransferAllEventFetching: fetching events")

	eventsFromBatch1, _, err := storeBatch1.QueryAllEventByBlockRange(context.Background(), uint64(testEventStartBlock), uint64(testEventEndBlock))

	require.Nil(t, err, "err should be nil for storeBatch1 query")

	t.Log("TestE2EFlowTransferAllEventFetching: fetching events with batch  1 got ", len(eventsFromBatch1))

	eventsFromBatch200, _, err := storeBatch200.QueryAllEventByBlockRange(context.Background(), uint64(testEventStartBlock), uint64(testEventEndBlock))

	require.Nil(t, err, "err should be nil for storeBatch200 query")

	t.Log("TestE2EFlowTransferAllEventFetching: fetching events with batch 200 got ", len(eventsFromBatch200))

	require.Equal(t, len(eventsFromBatch1), len(eventsFromBatch200), len(eventsFromBatch200))
}
