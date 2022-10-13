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

package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"
)

type ModelResponse struct {
	Text   string    `json:"text"`
	Vector []float32 `json:"vector"`
	Dim    int64     `json:"dim"`
}

// Client establishes a gRPC connection to a remote neuralmagic service
type Client struct {
	client *http.Client
	logger logrus.FieldLogger
}

// NewClient from gRPC discovery url to connect to a remote contextionary service
func NewClient(logger logrus.FieldLogger) (*Client, error) {
	return &Client{
		client: &http.Client{},
		logger: logger,
	}, nil
}

func (c *Client) Vectorize(text string) (*ModelResponse, error) {
	body := map[string]string{"text": text}
	b, err := json.Marshal(&body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal body: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, "http://localhost:8081", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	b, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var respBody ModelResponse
	err = json.Unmarshal(b, &respBody)
	if err != nil {
		return nil, err
	}

	return &respBody, nil
}
