package api

import "github.com/yourusername/emailworker/internal/db"

// ── Request structs ──────────────────────────────────────────────────────────

// CreateJobRequest binds the multipart form fields (not the file itself).
// `binding:"required,url"` tells Gin to validate: must be present AND a valid URL.
type CreateJobRequest struct {
	WebhookURL string `form:"webhook_url" binding:"required,url"`
}

// ── Response structs ─────────────────────────────────────────────────────────

// JobResponse wraps db.Job for a single-job API response.
// Embedding *db.Job means all of Job's JSON fields appear at the top level.
type JobResponse struct {
	*db.Job
}

// ListJobsResponse is the paginated envelope for GET /jobs.
type ListJobsResponse struct {
	Jobs       []*db.Job `json:"jobs"`
	Total      int       `json:"total"`
	Page       int       `json:"page"`
	Limit      int       `json:"limit"`
	TotalPages int       `json:"total_pages"`
}

// CreateJobResponse is returned after a successful POST /jobs.
type CreateJobResponse struct {
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// ErrorResponse is the single, consistent error envelope used everywhere.
// All errors look like: {"error": "some message"}
type ErrorResponse struct {
	Error string `json:"error"`
}
