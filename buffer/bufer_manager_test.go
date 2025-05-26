package buffer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferBasicOperations(t *testing.T) {
	fm, _, bm, cleanup := setupTest(t)
	defer cleanup()

	// Create a new block
	filename := fk.RandomStringWithLength(8)
	blk, err := fm.Append(filename)
	require.NoError(t, err)

	// Test initial available buffers
	assert.Equal(t, testBufferSize, bm.Available())

	// Pin a block
	buf, err := bm.Pin(blk)
	require.NoError(t, err)

	// Test available buffers after pin
	assert.Equal(t, testBufferSize-1, bm.Available())

	// Write some data to the buffer
	page := buf.Contents
	page.SetString(0, "test data")
	buf.SetModified(1, 1) // Transaction 1, LSN 1

	// Unpin the buffer
	bm.Unpin(buf)

	// Test available buffers after unpin
	assert.Equal(t, testBufferSize, bm.Available())

	// Flush all buffers for transaction 1
	err = bm.FlushAll(1)
	require.NoError(t, err)

	// Verify the data was written to disk
	page2, err := file.NewPage(fm.BlockSize())
	require.NoError(t, err)
	err = fm.Read(blk, page2)
	require.NoError(t, err)
	assert.Equal(t, "test data", page2.String(0))
}

func TestBufferReuse(t *testing.T) {
	fm, _, bm, cleanup := setupTest(t)
	defer cleanup()

	// Create more blocks than buffers
	numBlocks := testBufferSize + 5
	blocks := make([]*file.BlockId, numBlocks)
	for i := range numBlocks {
		filename := fmt.Sprintf("testfile_%d", i)
		blk, err := fm.Append(filename)
		require.NoError(t, err)
		blocks[i] = blk
	}

	// Pin all buffers
	buffers := make([]*Buffer, testBufferSize)
	for i := range testBufferSize {
		buff, err := bm.Pin(blocks[i])
		require.NoError(t, err)
		buffers[i] = buff
	}

	// Available should be 0
	assert.Equal(t, 0, bm.Available())

	// Unpin first buffer
	bm.Unpin(buffers[1])

	// Pin a new block, should reuse the buffer
	buff, err := bm.Pin(blocks[testBufferSize+1])
	require.NoError(t, err)

	require.EqualValues(t, buffers[1], buff) // Should reuse the unpinned buffer

	// Available should still be 0
	assert.Equal(t, 0, bm.Available())
}

func TestBufferConcurrency(t *testing.T) {
	fm, _, bm, cleanup := setupTest(t)
	defer cleanup()

	numWorkers := testBufferSize + 5
	numOperations := numWorkers * 10
	var wg sync.WaitGroup

	// Create blocks for testing
	blocks := make([]*file.BlockId, numWorkers*2)
	for i := range numWorkers * 2 {
		filename := fmt.Sprintf("testfile_%d", i)
		blk, err := fm.Append(filename)
		require.NoError(t, err)
		blocks[i] = blk
	}

	// Start workers
	for workerId := range numWorkers {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			// Each worker performs multiple pin/unpin operations
			for i := range numOperations {
				// Use different blocks to create contention
				blockIndex := (workerId + i) % len(blocks)
				buf, err := bm.Pin(blocks[blockIndex])
				if err != nil {
					// Some pins may fail due to timeout, which is expected
					// when there's heavy contention
					continue
				}

				// Simulate some work
				time.Sleep(time.Duration(fk.IntBetween(1, 10)) * time.Millisecond)

				// Modify the buffer occasionally
				if i%10 == 0 {
					buf.SetModified(workerId, i)
				}

				// Unpin the buffer
				bm.Unpin(buf)
			}
		}(workerId)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Verify that all buffers are eventually released
	assert.Equal(t, testBufferSize, bm.Available())
}

func TestBufferPinTimeout(t *testing.T) {
	fm, _, bm, cleanup := setupTest(t)
	defer cleanup()

	// Pin all buffers
	buffers := make([]*Buffer, testBufferSize)
	for i := range testBufferSize {
		filename := fmt.Sprintf("testfile_%d", i)
		blk, err := fm.Append(filename)
		require.NoError(t, err)

		buffers[i], err = bm.Pin(blk)
		require.NoError(t, err)
	}

	// Available should be 0
	assert.Equal(t, 0, bm.Available())

	// Try to pin another block, should timeout
	blk, err := fm.Append("testfile_timeout")
	require.NoError(t, err)

	// Start a goroutine to pin the block
	pinDone := make(chan struct{})
	var pinErr error
	go func() {
		_, pinErr = bm.Pin(blk)
		close(pinDone)
	}()

	// Wait for the pin to timeout or complete
	select {
	case <-pinDone:
		// Should have timed out
		assert.Error(t, pinErr)
		assert.Contains(t, pinErr.Error(), "PIN_TIMEOUT")
	case <-time.After(testMaxTime + time.Second):
		t.Fatal("Pin operation did not timeout as expected")
	}

	// Unpin all buffers
	for _, buf := range buffers {
		bm.Unpin(buf)
	}

	// Available should be testBufferSize again
	assert.Equal(t, testBufferSize, bm.Available())
}

func TestBufferWaiting(t *testing.T) {
	fm, _, bm, cleanup := setupTest(t)
	defer cleanup()

	// Pin a buffer
	filename := fk.RandomStringWithLength(8)
	blk, err := fm.Append(filename)
	require.NoError(t, err)
	buf, err := bm.Pin(blk)
	require.NoError(t, err)

	// Start a goroutine to pin the same block
	pinDone := make(chan *Buffer)
	go func() {
		buf, err := bm.Pin(blk)
		require.NoError(t, err)
		pinDone <- buf
	}()

	// Unpin the buffer after a short delay
	time.Sleep(time.Duration(fk.IntBetween(50, 200)) * time.Millisecond)
	bm.Unpin(buf)

	// Wait for the second pin to complete
	select {
	case buf2 := <-pinDone:
		// Unpin the buffer
		bm.Unpin(buf2)
	case <-time.After(time.Second):
		t.Fatal("Second Pin operation did not complete as expected")
	}

	// Available should be testBufferSize again
	assert.Equal(t, testBufferSize, bm.Available())
}

func TestConcurrentFlushAll(t *testing.T) {
	fm, _, bm, cleanup := setupTest(t)
	defer cleanup()

	numWorkers := 5
	numBlocks := 20
	var wg sync.WaitGroup

	// Create blocks for testing
	blocks := make([]*file.BlockId, numBlocks)
	for i := range numBlocks {
		filename := fmt.Sprintf("testfile_%d", i)
		blk, err := fm.Append(filename)
		require.NoError(t, err)
		blocks[i] = blk
	}

	// Start pin/unpin workers
	for w := range numWorkers {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			// Each worker performs multiple pin/unpin/modify operations
			for i := range 50 {
				blockIndex := (workerId + i) % len(blocks)
				buf, err := bm.Pin(blocks[blockIndex])
				if err != nil {
					continue
				}

				// Modify some buffers
				if i%3 == 0 {
					page := buf.Contents
					page.SetString(0, fmt.Sprintf("data-%d-%d", workerId, i))
					buf.SetModified(workerId, i)
				}

				// Simulate some work
				time.Sleep(time.Duration(fk.IntBetween(50, 100)) * time.Millisecond)

				// Unpin the buffer
				bm.Unpin(buf)
			}
		}(w)
	}

	// Start FlushAll workers
	for w := range 3 {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			// Each worker performs multiple FlushAll operations
			for i := range 10 {
				// Flush all buffers for a random transaction
				txNum := i % numWorkers
				err := bm.FlushAll(txNum)
				require.NoError(t, err)

				// Simulate some work
				time.Sleep(time.Duration(fk.IntBetween(5, 20)) * time.Millisecond)
			}
		}(w)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Verify that all buffers are eventually released
	assert.Equal(t, testBufferSize, bm.Available())
}
