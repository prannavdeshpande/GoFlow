package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// JobRecipientInput represents a single row parsed from the uploaded CSV.
type JobRecipientInput struct {
	Email string            `json:"email"`
	Data  map[string]string `json:"data"`
}

// JobRecipient mirrors a row in the job_recipients table.
type JobRecipient struct {
	ID        string            `json:"id"`
	JobID     string            `json:"job_id"`
	Email     string            `json:"email"`
	Data      map[string]string `json:"data"`
	Status    string            `json:"status"`
	Attempts  int               `json:"attempts"`
	ErrorMsg  *string           `json:"error_msg,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`
}

// CreateJobWithRecipients inserts a job and all CSV rows inside one transaction.
func CreateJobWithRecipients(ctx context.Context, pool *pgxpool.Pool, webhookURL, csvPath string, recipients []JobRecipientInput) (*Job, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("CreateJobWithRecipients begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	const jobQuery = `
		INSERT INTO jobs (webhook_url, csv_path, total_rows)
		VALUES ($1, $2, $3)
		RETURNING id, status, webhook_url, csv_path, total_rows, error_msg, created_at, updated_at`

	job := &Job{}
	err = tx.QueryRow(ctx, jobQuery, webhookURL, csvPath, len(recipients)).Scan(
		&job.ID, &job.Status, &job.WebhookURL, &job.CSVPath,
		&job.TotalRows, &job.ErrorMsg, &job.CreatedAt, &job.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("CreateJobWithRecipients insert job: %w", err)
	}

	const recipientQuery = `
		INSERT INTO job_recipients (job_id, email, data)
		VALUES ($1, $2, $3)`

	for _, recipient := range recipients {
		payload, marshalErr := json.Marshal(recipient.Data)
		if marshalErr != nil {
			return nil, fmt.Errorf("CreateJobWithRecipients marshal row: %w", marshalErr)
		}
		if _, err = tx.Exec(ctx, recipientQuery, job.ID, recipient.Email, payload); err != nil {
			return nil, fmt.Errorf("CreateJobWithRecipients insert row: %w", err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("CreateJobWithRecipients commit: %w", err)
	}

	return job, nil
}

// GetJobRecipientsByJobID returns all recipient rows for a job.
func GetJobRecipientsByJobID(ctx context.Context, pool *pgxpool.Pool, jobID string) ([]*JobRecipient, error) {
	const q = `
		SELECT id, job_id, email, data, status, attempts, error_msg, created_at, updated_at
		FROM job_recipients
		WHERE job_id = $1
		ORDER BY created_at ASC`

	rows, err := pool.Query(ctx, q, jobID)
	if err != nil {
		return nil, fmt.Errorf("GetJobRecipientsByJobID query: %w", err)
	}
	defer rows.Close()

	var recipients []*JobRecipient
	for rows.Next() {
		recipient := &JobRecipient{}
		var payload []byte
		if err := rows.Scan(
			&recipient.ID, &recipient.JobID, &recipient.Email, &payload,
			&recipient.Status, &recipient.Attempts, &recipient.ErrorMsg,
			&recipient.CreatedAt, &recipient.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("GetJobRecipientsByJobID scan: %w", err)
		}
		if len(payload) > 0 {
			if err := json.Unmarshal(payload, &recipient.Data); err != nil {
				return nil, fmt.Errorf("GetJobRecipientsByJobID unmarshal: %w", err)
			}
		}
		recipients = append(recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetJobRecipientsByJobID rows: %w", err)
	}
	return recipients, nil
}

// UpdateJobRecipientStatus updates a recipient row's status, attempt count, and error message.
func UpdateJobRecipientStatus(ctx context.Context, pool *pgxpool.Pool, id, status string, attempts int, errorMsg *string) (*JobRecipient, error) {
	const q = `
		UPDATE job_recipients
		SET status = $2, attempts = $3, error_msg = $4, updated_at = NOW()
		WHERE id = $1
		RETURNING id, job_id, email, data, status, attempts, error_msg, created_at, updated_at`

	recipient := &JobRecipient{}
	var payload []byte
	err := pool.QueryRow(ctx, q, id, status, attempts, errorMsg).Scan(
		&recipient.ID, &recipient.JobID, &recipient.Email, &payload,
		&recipient.Status, &recipient.Attempts, &recipient.ErrorMsg,
		&recipient.CreatedAt, &recipient.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("UpdateJobRecipientStatus: %w", err)
	}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &recipient.Data); err != nil {
			return nil, fmt.Errorf("UpdateJobRecipientStatus unmarshal: %w", err)
		}
	}
	return recipient, nil
}