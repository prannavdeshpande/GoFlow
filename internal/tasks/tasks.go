package tasks

import (
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"
)

// TypeEmailBatch is the queue task identifier.
// asynq uses this string to route tasks to the correct processor.
const TypeEmailBatch = "email:batch"

// EmailBatchPayload is JSON-encoded and stored in Redis as the task payload.
type EmailBatchPayload struct {
	JobID      string `json:"job_id"`
	CSVPath    string `json:"csv_path"`
	WebhookURL string `json:"webhook_url"`
}

// NewEmailBatchTask creates an asynq.Task ready to be enqueued.
func NewEmailBatchTask(jobID, csvPath, webhookURL string) (*asynq.Task, error) {
	payload, err := json.Marshal(EmailBatchPayload{
		JobID:      jobID,
		CSVPath:    csvPath,
		WebhookURL: webhookURL,
	})
	if err != nil {
		return nil, fmt.Errorf("NewEmailBatchTask marshal: %w", err)
	}
	return asynq.NewTask(TypeEmailBatch, payload), nil
}
