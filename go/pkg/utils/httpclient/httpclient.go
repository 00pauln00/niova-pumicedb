package httpclient

import (
	"bytes"
	"fmt"
	"io"
	"net/http"

	log "github.com/sirupsen/logrus"
)

func ServiceRequest(req *http.Request) ([]byte, error) {
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http client request failure: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, nil
	}

	// Everything else is an error
	return nil, fmt.Errorf("http error: status %d, body: %s", resp.StatusCode, body)
}

func HTTP_Request(requestBody []byte, address string, put bool) ([]byte, error) {
	var request *http.Request
	var err error

	connectionAddress := "http://" + address
	if put {
		request, err = http.NewRequest(http.MethodPut, connectionAddress, bytes.NewBuffer(requestBody))
	} else {
		request, err = http.NewRequest(http.MethodGet, connectionAddress, bytes.NewBuffer(requestBody))
	}
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return ServiceRequest(request)
}
