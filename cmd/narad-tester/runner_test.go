package main

import (
	"errors"
	"net/http"
	"testing"
)

func TestRetryableSetupError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "network error",
			err:  errors.New("connection refused"),
			want: true,
		},
		{
			name: "server error",
			err:  &apiStatusError{StatusCode: http.StatusInternalServerError},
			want: true,
		},
		{
			name: "too many requests",
			err:  &apiStatusError{StatusCode: http.StatusTooManyRequests},
			want: true,
		},
		{
			name: "bad request",
			err:  &apiStatusError{StatusCode: http.StatusBadRequest},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := retryableSetupError(tt.err); got != tt.want {
				t.Fatalf("retryableSetupError() = %v, want %v", got, tt.want)
			}
		})
	}
}
