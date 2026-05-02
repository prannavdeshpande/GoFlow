package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/smtp"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourusername/emailworker/internal/db"
	"github.com/yourusername/emailworker/internal/tasks"
	"github.com/yourusername/emailworker/internal/webhook"
)

const defaultWorkerCount = 5

// Processor owns the asynq server and SMTP configuration for background work.
type Processor struct {
	server    *asynq.Server
	db        *pgxpool.Pool
	smtpHost  string
	smtpPort  string
	smtpUser  string
	smtpPass  string
	smtpFrom  string
	workerCnt int
}

// NewProcessorFromEnv builds a Processor from the current process environment.
func NewProcessorFromEnv(pool *pgxpool.Pool) (*Processor, error) {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	workerCount := defaultWorkerCount
	if raw := os.Getenv("WORKER_COUNT"); raw != "" {
		count, err := strconv.Atoi(raw)
		if err != nil || count < 1 {
			return nil, fmt.Errorf("invalid WORKER_COUNT: %q", raw)
		}
		workerCount = count
	}

	smtpHost := os.Getenv("SMTP_HOST")
	if smtpHost == "" {
		return nil, fmt.Errorf("SMTP_HOST is not set")
	}
	smtpPort := os.Getenv("SMTP_PORT")
	if smtpPort == "" {
		smtpPort = "587"
	}
	smtpUser := os.Getenv("SMTP_USERNAME")
	smtpPass := os.Getenv("SMTP_PASSWORD")
	smtpFrom := os.Getenv("SMTP_FROM")
	if smtpFrom == "" {
		smtpFrom = smtpUser
	}
	if smtpFrom == "" {
		return nil, fmt.Errorf("SMTP_FROM is not set")
	}

	server := asynq.NewServer(
		asynq.RedisClientOpt{Addr: redisAddr},
		asynq.Config{Concurrency: workerCount},
	)

	return &Processor{
		server:    server,
		db:        pool,
		smtpHost:  smtpHost,
		smtpPort:  smtpPort,
		smtpUser:  smtpUser,
		smtpPass:  smtpPass,
		smtpFrom:  smtpFrom,
		workerCnt: workerCount,
	}, nil
}

// Run starts the worker pool and blocks until it receives SIGINT or SIGTERM.
func (p *Processor) Run(ctx context.Context) error {
	mux := asynq.NewServeMux()
	mux.HandleFunc(tasks.TypeEmailBatch, p.handleEmailBatch)

	shutdownCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("INFO  worker pool starting with %d goroutines", p.workerCnt)
		if err := p.server.Run(mux); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-shutdownCtx.Done()
		log.Println("INFO  shutting down worker pool")
		p.server.Shutdown()
	}()

	select {
	case err := <-errCh:
		stop()
		wg.Wait()
		return err
	case <-shutdownCtx.Done():
		wg.Wait()
		return nil
	}
}

func (p *Processor) handleEmailBatch(ctx context.Context, task *asynq.Task) error {
	var payload tasks.EmailBatchPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("decode task payload: %w", err)
	}

	job, err := db.GetJobByID(ctx, p.db, payload.JobID)
	if err != nil {
		return err
	}

	recipients, err := db.GetJobRecipientsByJobID(ctx, p.db, payload.JobID)
	if err != nil {
		return err
	}
	if len(recipients) == 0 {
		msg := "no recipients found"
		_, _ = db.UpdateJobStatus(ctx, p.db, job.ID, "failed", &msg)
		return nil
	}

	processing := "processing"
	if _, err := db.UpdateJobStatus(ctx, p.db, job.ID, processing, nil); err != nil {
		return err
	}

	anyFailed := false
	failedCount := 0
	for _, recipient := range recipients {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if recipient.Status == "sent" {
			continue
		}
		if err := p.processRecipient(ctx, job.ID, recipient); err != nil {
			anyFailed = true
			failedCount++
			log.Printf("WARN  recipient %s failed: %v", recipient.Email, err)
		}
	}

	status := "done"
	var errorMsg *string
	if anyFailed {
		status = "failed"
		msg := "one or more recipients failed"
		errorMsg = &msg
	}
	if _, err := db.UpdateJobStatus(ctx, p.db, job.ID, status, errorMsg); err != nil {
		return err
	}

	if job.WebhookURL != "" {
		payload := webhook.WebhookPayload{
			JobID:           job.ID,
			Status:          status,
			TotalRecipients: len(recipients),
			FailedCount:     failedCount,
			CompletedAt:     time.Now(),
		}
		if err := webhook.Send(ctx, job.WebhookURL, payload); err != nil {
			log.Printf("WARN  webhook send failed: %v", err)
		}
	}

	return nil
}

func (p *Processor) processRecipient(ctx context.Context, jobID string, recipient *db.JobRecipient) error {
	subject := fmt.Sprintf("Bulk email job %s", jobID)
	body, err := buildEmailBody(jobID, recipient)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if _, err := db.UpdateJobRecipientStatus(ctx, p.db, recipient.ID, "processing", attempt, nil); err != nil {
			return err
		}

		lastErr = p.sendEmail(recipient.Email, subject, body)
		if lastErr == nil {
			if _, err := db.UpdateJobRecipientStatus(ctx, p.db, recipient.ID, "sent", attempt, nil); err != nil {
				return err
			}
			return nil
		}

		if attempt < 3 {
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	msg := lastErr.Error()
	_, _ = db.UpdateJobRecipientStatus(ctx, p.db, recipient.ID, "failed", 3, &msg)
	return lastErr
}

func buildEmailBody(jobID string, recipient *db.JobRecipient) (string, error) {
	pretty, err := json.MarshalIndent(recipient.Data, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal email body: %w", err)
	}

	var builder strings.Builder
	builder.WriteString("Job ID: ")
	builder.WriteString(jobID)
	builder.WriteString("\r\n")
	builder.WriteString("Recipient: ")
	builder.WriteString(recipient.Email)
	builder.WriteString("\r\n\r\n")
	builder.WriteString("Row data:\r\n")
	builder.Write(pretty)
	builder.WriteString("\r\n")
	return builder.String(), nil
}

func (p *Processor) sendEmail(to, subject, body string) error {
	var auth smtp.Auth
	if p.smtpUser != "" || p.smtpPass != "" {
		auth = smtp.PlainAuth("", p.smtpUser, p.smtpPass, p.smtpHost)
	}

	var msg bytes.Buffer
	msg.WriteString("From: ")
	msg.WriteString(p.smtpFrom)
	msg.WriteString("\r\n")
	msg.WriteString("To: ")
	msg.WriteString(to)
	msg.WriteString("\r\n")
	msg.WriteString("Subject: ")
	msg.WriteString(subject)
	msg.WriteString("\r\n")
	msg.WriteString("MIME-Version: 1.0\r\n")
	msg.WriteString("Content-Type: text/plain; charset=UTF-8\r\n\r\n")
	msg.WriteString(body)

	addr := net.JoinHostPort(p.smtpHost, p.smtpPort)
	if err := smtp.SendMail(addr, auth, p.smtpFrom, []string{to}, msg.Bytes()); err != nil {
		return fmt.Errorf("send mail: %w", err)
	}
	return nil
}