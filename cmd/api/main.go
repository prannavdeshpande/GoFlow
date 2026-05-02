package main

import (
	"context"
	"log"
	"os"

	"github.com/hibiken/asynq"
	"github.com/joho/godotenv"
	"github.com/yourusername/emailworker/internal/api"
	"github.com/yourusername/emailworker/internal/db"
)

func main() {
	// Load .env file into environment variables.
	// The underscore _ intentionally discards the error:
	// if .env doesn't exist (e.g. in production), that's fine.
	_ = godotenv.Load()

	// context.Background() is the root context — a signal-passing mechanism
	// that lets you cancel operations. Required by pgx functions.
	ctx := context.Background()

	// Connect to PostgreSQL
	pool, err := db.Connect(ctx)
	if err != nil {
		log.Fatalf("FATAL DB connect: %v", err)
	}
	defer pool.Close() // runs when main() returns, closing all connections

	// Connect the asynq client to Redis
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	queueClient := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr})
	defer queueClient.Close()

	// JWT secret — must be long and random in production!
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		jwtSecret = "dev-secret-change-me-in-production"
		log.Println("WARN  JWT_SECRET not set, using insecure default")
	}

	// Port
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Build and start the server
	srv := api.NewServer(pool, queueClient, []byte(jwtSecret))
	log.Printf("INFO  API server starting on http://localhost:%s", port)
	if err := srv.Run(":" + port); err != nil {
		log.Fatalf("FATAL server: %v", err)
	}
}
