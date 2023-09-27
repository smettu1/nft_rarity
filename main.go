package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

const COLLECTION = "azuki"
const COLOR_GREEN = "\033[32m"
const COLOR_RED = "\033[31m"
const COLOR_RESET = "\033[0m"
const MAX_ELEMENT_SIZE = 5

var logger *log.Logger = log.New(os.Stdout, "", log.Ldate|log.Ltime)

type RarityScorecard struct {
	rarity float64
	id     int
}

type Collection struct {
	count int
	url   string
}

// Define a custom struct to hold both rarity and token ID.
type RarityWithID struct {
	rarity float64
	id     int
}

// Define a type that represents a max heap of RarityWithID.
type MaxHeap []RarityWithID

func (h MaxHeap) Len() int           { return len(h) }
func (h MaxHeap) Less(i, j int) bool { return h[i].rarity > h[j].rarity } // Max heap, so greater rarity is "less"
func (h MaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *MaxHeap) Push(x interface{}) {
	*h = append(*h, x.(RarityWithID))
}

func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Aggregates encapsulates the cumulative count of all summarized attributes
// and the cumulative sum of attributes.
type Aggregates struct {
	aggr map[string]map[string]int // Nested map for counting attributes
	mu   sync.Mutex                // Mutex for synchronization
}

type Token struct {
	id    int
	attrs map[string]string
}

// UpdateTokenData updates the Aggregates with data from a Token.
func (a *Aggregates) UpdateTokenData(token *Token) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for key, value := range token.attrs {
		// Check if the key exists in the outer map
		if subMap, exists := a.aggr[key]; !exists {
			// Key doesn't exist, so create it with an inner map
			a.aggr[key] = make(map[string]int)
			a.aggr[key][value] = 1
		} else {
			// Check if the sub-key exists in the inner map
			if _, subExists := subMap[value]; !subExists {
				// Sub-key doesn't exist, so create it
				a.aggr[key][value] = 1
			} else {
				// Sub-key exists, so increment its count
				a.aggr[key][value]++
			}
		}
	}
}

// UpdateRarity calculates the rarity of a token based on attributes and updates the top 5 rarity scorecards.
func (a *Aggregates) UpdateRarity(token *Token, maxHeap *MaxHeap) {
	rarity := 0.0

	// Calculate the rarity based on attributes
	for key, value := range token.attrs {
		rarity += 1 / (float64(len(a.aggr[key]) * a.aggr[key][value]))
	}

	// Create a RarityWithID
	r := RarityWithID{id: token.id, rarity: rarity}

	// Push the RarityWithID onto the heap
	heap.Push(maxHeap, r)
}

// getTokenWithRetry performs an HTTP GET request with retry logic.
func getTokenWithRetry(client *http.Client, tid int, colUrl string, tokens chan<- *Token, maxRetries int, url string) {
	var token *Token
	var err error

	for retry := 0; retry <= maxRetries; retry++ {
		if retry > 0 {
			time.Sleep(10 * time.Second) // Sleep for 10 seconds between retries
		}
		token, err = getToken(client, tid, url)
		if err == nil {
			break // Successful request, break out of retry loop.
		}
		logger.Println(string(COLOR_RED), fmt.Sprintf("Error getting token %d (Retry %d):", tid, retry), err, string(COLOR_RESET))
	}

	tokens <- token
}

// getToken performs a single HTTP GET request.
func getToken(client *http.Client, tid int, url string) (*Token, error) {
	res, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status code: %d", res.StatusCode)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var attrs map[string]string
	if err := json.Unmarshal(body, &attrs); err != nil {
		return nil, err
	}

	return &Token{
		id:    tid,
		attrs: attrs,
	}, nil
}

func getTokens(col Collection, aggr *Aggregates, maxHeap *MaxHeap, maxThreads int, url string) {
	tokens := make([]*Token, col.count)
	var wg sync.WaitGroup
	tokensChan := make(chan *Token, col.count)
	maxRetries := 3

	// Create an HTTP client with timeouts.
	client := &http.Client{
		Timeout: time.Second * 100, // Timeout after 100 seconds
	}
	sem := semaphore.NewWeighted(int64(maxThreads)) // Adjust the semaphore limit as needed

	for i := 0; i < col.count; i++ {
		tid := i
		wg.Add(1)
		sem.Acquire(context.Background(), 1)
		// log
		go func() {
			defer sem.Release(1)
			defer wg.Done()
			logger.Println(string(COLOR_GREEN), fmt.Sprintf("Getting token %d", tid), string(COLOR_RESET))
			getTokenWithRetry(client, tid, col.url, tokensChan, maxRetries, url)
		}()
	}

	go func() {
		wg.Wait()
		close(tokensChan)
	}()

	// Update the token metadata with counts.
	for tok := range tokensChan {
		tokens[tok.id] = tok
		aggr.UpdateTokenData(tok)
	}

	// Now calculate rarity
	for _, tok := range tokens {
		aggr.UpdateRarity(tok, maxHeap)
	}
}

// Create a new Aggregator with the aggr field initialized
func makeNewAggregator() *Aggregates {
	aggregator := &Aggregates{
		aggr: make(map[string]map[string]int),
	}
	return aggregator
}

func main() {
	azuki := Collection{
		count: 10000,
		url:   "azuki1",
	}
	aggr := makeNewAggregator()
	// Create an empty max heap.
	maxHeap := &MaxHeap{}
	var count int
	var url string
	flag.IntVar(&count, "n", 100, "number of threads ran in parallel")
	flag.StringVar(&url, "url", "https://google.com", "URL for parsing NFT's")
	flag.Parse()

	getTokens(azuki, aggr, maxHeap, count, url)

	for i := 0; i < MAX_ELEMENT_SIZE; i++ {
		item := heap.Pop(maxHeap).(RarityWithID)
		fmt.Printf("Top rarity score %.4f for Token ID %d\n", item.rarity, item.id)
	}
}
