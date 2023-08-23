// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otelarrowreceiver // import "github.com/open-telemetry/otel-arrow/collector/receiver/otelarrowreceiver"

import (
	"io"
	"mime"
	"net/http"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/open-telemetry/otel-arrow/collector/receiver/otelarrowreceiver/internal/logs"
	"github.com/open-telemetry/otel-arrow/collector/receiver/otelarrowreceiver/internal/metrics"
	"github.com/open-telemetry/otel-arrow/collector/receiver/otelarrowreceiver/internal/trace"
)

// Pre-computed status with code=Internal to be used in case of a marshaling error.
var fallbackMsg = []byte(`{"code": 13, "message": "failed to marshal error message"}`)

const fallbackContentType = "application/json"

func handleTraces(resp http.ResponseWriter, req *http.Request, tracesReceiver *trace.Receiver, encoder encoder) {
	body, ok := readAndCloseBody(resp, req, encoder)
	if !ok {
		return
	}

	otlpReq, err := encoder.unmarshalTracesRequest(body)
	if err != nil {
		writeError(resp, encoder, err, http.StatusBadRequest)
		return
	}

	otlpResp, err := tracesReceiver.Export(req.Context(), otlpReq)
	if err != nil {
		writeError(resp, encoder, err, http.StatusInternalServerError)
		return
	}

	msg, err := encoder.marshalTracesResponse(otlpResp)
	if err != nil {
		writeError(resp, encoder, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, encoder.contentType(), http.StatusOK, msg)
}

func handleMetrics(resp http.ResponseWriter, req *http.Request, metricsReceiver *metrics.Receiver, encoder encoder) {
	body, ok := readAndCloseBody(resp, req, encoder)
	if !ok {
		return
	}

	otlpReq, err := encoder.unmarshalMetricsRequest(body)
	if err != nil {
		writeError(resp, encoder, err, http.StatusBadRequest)
		return
	}

	otlpResp, err := metricsReceiver.Export(req.Context(), otlpReq)
	if err != nil {
		writeError(resp, encoder, err, http.StatusInternalServerError)
		return
	}

	msg, err := encoder.marshalMetricsResponse(otlpResp)
	if err != nil {
		writeError(resp, encoder, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, encoder.contentType(), http.StatusOK, msg)
}

func handleLogs(resp http.ResponseWriter, req *http.Request, logsReceiver *logs.Receiver, encoder encoder) {
	body, ok := readAndCloseBody(resp, req, encoder)
	if !ok {
		return
	}

	otlpReq, err := encoder.unmarshalLogsRequest(body)
	if err != nil {
		writeError(resp, encoder, err, http.StatusBadRequest)
		return
	}

	otlpResp, err := logsReceiver.Export(req.Context(), otlpReq)
	if err != nil {
		writeError(resp, encoder, err, http.StatusInternalServerError)
		return
	}

	msg, err := encoder.marshalLogsResponse(otlpResp)
	if err != nil {
		writeError(resp, encoder, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, encoder.contentType(), http.StatusOK, msg)
}

func readAndCloseBody(resp http.ResponseWriter, req *http.Request, encoder encoder) ([]byte, bool) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		writeError(resp, encoder, err, http.StatusBadRequest)
		return nil, false
	}
	if err = req.Body.Close(); err != nil {
		writeError(resp, encoder, err, http.StatusBadRequest)
		return nil, false
	}
	return body, true
}

// writeError encodes the HTTP error inside a rpc.Status message as required by the OTLP protocol.
func writeError(w http.ResponseWriter, encoder encoder, err error, statusCode int) {
	s, ok := status.FromError(err)
	if !ok {
		s = errorMsgToStatus(err.Error(), statusCode)
	}
	writeStatusResponse(w, encoder, statusCode, s.Proto())
}

// errorHandler encodes the HTTP error message inside a rpc.Status message as required
// by the OTLP protocol.
func errorHandler(w http.ResponseWriter, r *http.Request, errMsg string, statusCode int) {
	s := errorMsgToStatus(errMsg, statusCode)
	switch getMimeTypeFromContentType(r.Header.Get("Content-Type")) {
	case pbContentType:
		writeStatusResponse(w, pbEncoder, statusCode, s.Proto())
		return
	case jsonContentType:
		writeStatusResponse(w, jsEncoder, statusCode, s.Proto())
		return
	}
	writeResponse(w, fallbackContentType, http.StatusInternalServerError, fallbackMsg)
}

func writeStatusResponse(w http.ResponseWriter, encoder encoder, statusCode int, rsp *spb.Status) {
	msg, err := encoder.marshalStatus(rsp)
	if err != nil {
		writeResponse(w, fallbackContentType, http.StatusInternalServerError, fallbackMsg)
		return
	}

	writeResponse(w, encoder.contentType(), statusCode, msg)
}

func writeResponse(w http.ResponseWriter, contentType string, statusCode int, msg []byte) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(statusCode)
	// Nothing we can do with the error if we cannot write to the response.
	_, _ = w.Write(msg)
}

func errorMsgToStatus(errMsg string, statusCode int) *status.Status {
	if statusCode == http.StatusBadRequest {
		return status.New(codes.InvalidArgument, errMsg)
	}
	return status.New(codes.Unknown, errMsg)
}

func getMimeTypeFromContentType(contentType string) string {
	mediatype, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	return mediatype
}