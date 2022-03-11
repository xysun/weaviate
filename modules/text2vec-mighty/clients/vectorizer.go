//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/modules/text2vec-mighty/ent"
	"github.com/sirupsen/logrus"
)

type vectorizer struct {
	origin     string
	httpClient *http.Client
	logger     logrus.FieldLogger
}

func New(origin string, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		origin:     origin,
		httpClient: &http.Client{},
		logger:     logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input string,
	config ent.VectorizationConfig) (*ent.VectorizationResult, error) {
	u := url.URL{}
	values := u.Query()
	values.Add("text", input)
	query := values.Encode()

	uri := v.url(fmt.Sprintf("/embeddings?%s", query))
	req, err := http.NewRequestWithContext(ctx, "GET", uri, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create POST request")
	}

	res, err := v.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send POST request")
	}
	defer res.Body.Close()

	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	if res.StatusCode < 299 {

		var resBody vecResponse
		if err := json.Unmarshal(bodyBytes, &resBody); err != nil {
			return nil, errors.Wrap(err, "unmarshal mighty response body")
		}
		return &ent.VectorizationResult{
			Text:       input,
			Dimensions: resBody.Shape[1],
			Vector:     resBody.Outputs[0],
		}, nil

	}

	return nil, errors.Errorf("mighty inference: %s", bodyBytes)
}

func (v *vectorizer) url(path string) string {
	return fmt.Sprintf("%s%s", v.origin, path)
}

type vecRequest struct {
	Text string `json:"text"`
	// Dims   int              `json:"dims"`
	// Vector []float32        `json:"vector"`
	// Error  string           `json:"error"`
	// Config vecRequestConfig `json:"config"`
}

type vecResponse struct {
	Shape   []int       `json:"shape"`
	Outputs [][]float32 `json:"outputs"`
}

type vecRequestConfig struct {
	PoolingStrategy string `json:"pooling_strategy"`
}
