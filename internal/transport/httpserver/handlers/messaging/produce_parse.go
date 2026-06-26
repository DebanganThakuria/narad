package messaging

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/buger/jsonparser"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

func decodeProduceRequest(s *handlers.Set, w http.ResponseWriter, body []byte) (produceRequest, bool) {
	req, err := parseProduceRequest(body)
	if err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return produceRequest{}, false
	}
	return req, true
}

func parseProduceRequest(body []byte) (produceRequest, error) {
	var req produceRequest

	key, err := jsonparser.GetString(body, "key")
	if err != nil {
		if !errors.Is(err, jsonparser.KeyPathNotFoundError) &&
			!errors.Is(err, jsonparser.NullValueError) {
			return req, err
		}
	}
	req.Key = key

	message, valueType, endOffset, err := jsonparser.Get(body, "message")
	if err != nil {
		if errors.Is(err, jsonparser.KeyPathNotFoundError) {
			return req, nil
		}
		return req, err
	}
	if valueType == jsonparser.String {
		message = rawStringValue(body, message, endOffset)
	}
	req.Message = json.RawMessage(message)
	return req, nil
}

func rawStringValue(body, unquoted []byte, endOffset int) []byte {
	start := endOffset - len(unquoted) - 2
	if start < 0 || endOffset > len(body) || body[start] != '"' {
		return nil
	}
	return body[start:endOffset]
}
