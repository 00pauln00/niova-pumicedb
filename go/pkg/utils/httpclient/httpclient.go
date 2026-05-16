package httpclient

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	log "github.com/sirupsen/logrus"
)

func service_Request(request *http.Request) ([]byte, error) {

	request.Header.Set("Content-Type", "application/json")
	httpClient := &http.Client{}

	response, err := httpClient.Do(request)
	if err != nil {
		log.Error("(HTTP CLIENT DO)", err)
		return nil, err
	}

	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		// Success responses
		return body, nil

	case response.StatusCode == 408:
		return nil, errors.New("request timeout")

	case response.StatusCode == 503:
		return nil, errors.New("service unavailable")

	case response.StatusCode >= 400 && response.StatusCode < 500:
		// return nil, fmt.Errorf("client error: %d - %s", response.StatusCode, string(body))
		// ignore client errors, as the serf sometimes requests to invalid ports, need to fix this!
		return nil, nil

	case response.StatusCode >= 500:
		return nil, fmt.Errorf("server error: %d - %s", response.StatusCode, string(body))

	default:
		return nil, fmt.Errorf("unexpected status code: %d - %s", response.StatusCode, string(body))
	}
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
	return service_Request(request)
}
