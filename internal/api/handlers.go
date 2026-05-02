package api

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yourusername/emailworker/internal/db"
	"github.com/yourusername/emailworker/internal/tasks"
)

// ── POST /jobs ───────────────────────────────────────────────────────────────

func (s *Server) createJob(c *gin.Context) {
	// Bind + validate the webhook_url form field.
	// ShouldBind reads multipart form data and checks the `binding:` tags.
	// If validation fails, err contains a human-readable description.
	var req CreateJobRequest
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	// Get the uploaded file from the multipart form.
	// FormFile returns a *multipart.FileHeader — metadata about the file.
	fileHeader, err := c.FormFile("csv_file")
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "csv_file field is required"})
		return
	}

	if filepath.Ext(fileHeader.Filename) != ".csv" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "only .csv files are accepted"})
		return
	}

	// Open the uploaded file into memory to count rows and validate CSV format.
	// fileHeader.Open() returns an io.ReadCloser — it must be closed when done.
	f, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to read uploaded file"})
		return
	}
	defer f.Close() // defer runs this line when the surrounding function returns

	recipients, err := parseRecipientsCSV(f)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}
	totalRows := len(recipients)

	// Save file to disk. UnixNano() prefix avoids filename collisions.
	uploadDir := "uploads"
	if err := os.MkdirAll(uploadDir, 0o755); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "storage error"})
		return
	}
	savePath := filepath.Join(uploadDir, fmt.Sprintf("%d_%s", time.Now().UnixNano(), fileHeader.Filename))
	if err := c.SaveUploadedFile(fileHeader, savePath); err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to save file"})
		return
	}
	defer func() {
		if err != nil {
			_ = os.Remove(savePath)
		}
	}()

	// Persist the job rows to PostgreSQL in the same transaction as the job record.
	job, err := db.CreateJobWithRecipients(c.Request.Context(), s.db, req.WebhookURL, savePath, recipients)
	if err != nil {
		log.Printf("ERROR createJob DB: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to create job"})
		return
	}

	// Enqueue the task in Redis via asynq.
	task, err := tasks.NewEmailBatchTask(job.ID, savePath, req.WebhookURL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to build task"})
		return
	}
	info, err := s.queue.Enqueue(task)
	if err != nil {
		log.Printf("ERROR createJob enqueue: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to enqueue job"})
		return
	}
	log.Printf("INFO  enqueued asynq_id=%s job_id=%s", info.ID, job.ID)

	c.JSON(http.StatusCreated, CreateJobResponse{
		JobID:   job.ID,
		Status:  job.Status,
		Message: fmt.Sprintf("Queued %d email recipients", totalRows),
	})
}

// ── GET /jobs/:id ─────────────────────────────────────────────────────────────

func (s *Server) getJob(c *gin.Context) {
	job, err := db.GetJobByID(c.Request.Context(), s.db, c.Param("id"))
	if errors.Is(err, db.ErrNotFound) {
		c.JSON(http.StatusNotFound, ErrorResponse{Error: "job not found"})
		return
	}
	if err != nil {
		log.Printf("ERROR getJob: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to fetch job"})
		return
	}
	c.JSON(http.StatusOK, JobResponse{job})
}

// ── GET /jobs ─────────────────────────────────────────────────────────────────

func (s *Server) listJobs(c *gin.Context) {
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))

	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 20
	}
	offset := (page - 1) * limit

	jobs, total, err := db.ListJobs(c.Request.Context(), s.db, limit, offset)
	if err != nil {
		log.Printf("ERROR listJobs: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to list jobs"})
		return
	}

	// Ensure JSON returns [] not null when there are no jobs.
	if jobs == nil {
		jobs = []*db.Job{}
	}

	totalPages := total / limit
	if total%limit != 0 {
		totalPages++
	}

	c.JSON(http.StatusOK, ListJobsResponse{
		Jobs:       jobs,
		Total:      total,
		Page:       page,
		Limit:      limit,
		TotalPages: totalPages,
	})
}

// ── DELETE /jobs/:id ──────────────────────────────────────────────────────────

func (s *Server) deleteJob(c *gin.Context) {
	id := c.Param("id")

	job, err := db.CancelJob(c.Request.Context(), s.db, id)
	if errors.Is(err, db.ErrNotFound) {
		// Could be: job doesn't exist, OR it exists but isn't pending.
		// Check which case to return the right status code.
		existing, dbErr := db.GetJobByID(c.Request.Context(), s.db, id)
		if errors.Is(dbErr, db.ErrNotFound) {
			c.JSON(http.StatusNotFound, ErrorResponse{Error: "job not found"})
			return
		}
		c.JSON(http.StatusConflict, ErrorResponse{
			Error: fmt.Sprintf("cannot cancel job with status '%s'", existing.Status),
		})
		return
	}
	if err != nil {
		log.Printf("ERROR deleteJob: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to cancel job"})
		return
	}

	c.JSON(http.StatusOK, JobResponse{job})
}

func parseRecipientsCSV(r io.Reader) ([]db.JobRecipientInput, error) {
	reader := csv.NewReader(r)
	header, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("CSV must have a header row plus at least one data row")
	}
	if err != nil {
		return nil, fmt.Errorf("invalid CSV header: %w", err)
	}

	normalizedHeader := make([]string, len(header))
	emailIndex := -1
	for i, value := range header {
		normalizedHeader[i] = strings.ToLower(strings.TrimSpace(value))
		if normalizedHeader[i] == "email" {
			emailIndex = i
		}
	}
	if emailIndex == -1 {
		return nil, fmt.Errorf("CSV must contain an email column")
	}

	var recipients []db.JobRecipientInput
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("invalid CSV: %w", err)
		}
		if len(record) != len(normalizedHeader) {
			return nil, fmt.Errorf("CSV rows must have the same number of columns as the header")
		}

		data := make(map[string]string, len(normalizedHeader))
		for i, column := range normalizedHeader {
			data[column] = strings.TrimSpace(record[i])
		}

		email := strings.TrimSpace(record[emailIndex])
		if email == "" {
			return nil, fmt.Errorf("CSV row is missing an email address")
		}

		recipients = append(recipients, db.JobRecipientInput{
			Email: email,
			Data:  data,
		})
	}

	if len(recipients) == 0 {
		return nil, fmt.Errorf("CSV must have a header row plus at least one data row")
	}

	return recipients, nil
}
