package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a queried row does not exist.
// We define our own error so callers don't need to import pgx directly.
var ErrNotFound = errors.New("record not found")

// Job mirrors a row in the jobs table.
// *string for ErrorMsg because it can be NULL in the database.
// omitempty means the field is omitted from JSON when it's nil.
type Job struct {
	ID         string    `json:"id"`
	Status     string    `json:"status"`
	WebhookURL string    `json:"webhook_url"`
	CSVPath    string    `json:"csv_path"`
	TotalRows  int       `json:"total_rows"`
	ErrorMsg   *string   `json:"error_msg,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// CreateJob inserts a new job row and returns the fully-populated Job.
// RETURNING lets PostgreSQL send back the generated values in one round-trip.
func CreateJob(ctx context.Context, pool *pgxpool.Pool, webhookURL, csvPath string, totalRows int) (*Job, error) {
	const q = `
		INSERT INTO jobs (webhook_url, csv_path, total_rows)
		VALUES ($1, $2, $3)
		RETURNING id, status, webhook_url, csv_path, total_rows, error_msg, created_at, updated_at`

	job := &Job{}
	err := pool.QueryRow(ctx, q, webhookURL, csvPath, totalRows).Scan(
		&job.ID, &job.Status, &job.WebhookURL, &job.CSVPath,
		&job.TotalRows, &job.ErrorMsg, &job.CreatedAt, &job.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("CreateJob: %w", err)
	}
	return job, nil
}

// GetJobByID fetches a single job by UUID. Returns ErrNotFound if missing.
func GetJobByID(ctx context.Context, pool *pgxpool.Pool, id string) (*Job, error) {
	const q = `
		SELECT id, status, webhook_url, csv_path, total_rows, error_msg, created_at, updated_at
		FROM jobs WHERE id = $1`

	job := &Job{}
	err := pool.QueryRow(ctx, q, id).Scan(
		&job.ID, &job.Status, &job.WebhookURL, &job.CSVPath,
		&job.TotalRows, &job.ErrorMsg, &job.CreatedAt, &job.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("GetJobByID: %w", err)
	}
	return job, nil
}

// ListJobs returns a page of jobs (newest first) and the total count.
func ListJobs(ctx context.Context, pool *pgxpool.Pool, limit, offset int) ([]*Job, int, error) {
	var total int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs").Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("ListJobs count: %w", err)
	}

	const q = `
		SELECT id, status, webhook_url, csv_path, total_rows, error_msg, created_at, updated_at
		FROM jobs ORDER BY created_at DESC LIMIT $1 OFFSET $2`

	rows, err := pool.Query(ctx, q, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("ListJobs query: %w", err)
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		job := &Job{}
		if err := rows.Scan(
			&job.ID, &job.Status, &job.WebhookURL, &job.CSVPath,
			&job.TotalRows, &job.ErrorMsg, &job.CreatedAt, &job.UpdatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("ListJobs scan: %w", err)
		}
		jobs = append(jobs, job)
	}
	return jobs, total, nil
}

// CancelJob sets status='cancelled' only when the job is still 'pending'.
// Returns ErrNotFound if no matching pending job exists.
func CancelJob(ctx context.Context, pool *pgxpool.Pool, id string) (*Job, error) {
	const q = `
		UPDATE jobs SET status = 'cancelled', updated_at = NOW()
		WHERE id = $1 AND status = 'pending'
		RETURNING id, status, webhook_url, csv_path, total_rows, error_msg, created_at, updated_at`

	job := &Job{}
	err := pool.QueryRow(ctx, q, id).Scan(
		&job.ID, &job.Status, &job.WebhookURL, &job.CSVPath,
		&job.TotalRows, &job.ErrorMsg, &job.CreatedAt, &job.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("CancelJob: %w", err)
	}
	return job, nil
}

// UpdateJobStatus sets the overall job status and optional error message.
func UpdateJobStatus(ctx context.Context, pool *pgxpool.Pool, id, status string, errorMsg *string) (*Job, error) {
	const q = `
		UPDATE jobs
		SET status = $2, error_msg = $3, updated_at = NOW()
		WHERE id = $1
		RETURNING id, status, webhook_url, csv_path, total_rows, error_msg, created_at, updated_at`

	job := &Job{}
	err := pool.QueryRow(ctx, q, id, status, errorMsg).Scan(
		&job.ID, &job.Status, &job.WebhookURL, &job.CSVPath,
		&job.TotalRows, &job.ErrorMsg, &job.CreatedAt, &job.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("UpdateJobStatus: %w", err)
	}
	return job, nil
}
