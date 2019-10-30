package contextionary

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/usecases/traverser"
)

type BertHTTPClient struct {
	endpoint string
}

func NewBertHTTPClient(endpoint string) *BertHTTPClient {
	return &BertHTTPClient{
		endpoint: endpoint,
	}
}

func (c *BertHTTPClient) IsStopWord(ctx context.Context, word string) (bool, error) {
	return false, fmt.Errorf("IsStopWord not supported")
}

func (c *BertHTTPClient) IsWordPresent(ctx context.Context, word string) (bool, error) {
	return false, fmt.Errorf("IsWordPresent not supported")
}

func (c *BertHTTPClient) SafeGetSimilarWordsWithCertainty(ctx context.Context, word string, certainty float32) ([]string, error) {
	return nil, fmt.Errorf("sgswwc not supported")
}

func (c *BertHTTPClient) NearestWordsByVector(ctx context.Context, vector []float32, n int, k int) ([]string, []float32, error) {
	return nil, nil, fmt.Errorf("NearestWordsByVector not supported")
}

func (c *BertHTTPClient) SchemaSearch(ctx context.Context, params traverser.SearchParams) (traverser.SearchResults, error) {
	return traverser.SearchResults{}, fmt.Errorf("SchemaSearch not supported")
}

func (c *BertHTTPClient) VectorForWord(ctx context.Context, word string) ([]float32, error) {
	return nil, fmt.Errorf("VectorForWord not supported")
}

func (c *BertHTTPClient) VectorForCorpi(ctx context.Context, corpi []string) ([]float32, error) {
	return nil, fmt.Errorf("VectorForCorpi not supported")
}

func (c *BertHTTPClient) Version(ctx context.Context) (string, error) {
	return "none", nil
}

func (c *BertHTTPClient) WordCount(ctx context.Context) (int64, error) {
	return 9000, nil
}
