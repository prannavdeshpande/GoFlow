// Package webhook will handle firing HTTP callbacks when a job completes.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// WebhookPayload represents the JSON payload sent to the configured webhook URL.
type WebhookPayload struct {
	JobID          string    `json:"job_id"`
	Status         string    `json:"status"`
	TotalRecipients int       `json:"total_recipients"`
	FailedCount    int       `json:"failed_count"`
	CompletedAt    time.Time `json:"completed_at"`
}

// Send posts the payload as JSON to the given URL. It returns an error
// when the HTTP request or non-2xx response occurs.
func Send(ctx context.Context, url string, payload WebhookPayload) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal webhook payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
