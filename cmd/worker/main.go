package main

import (
	"context"
	"log"

	"github.com/joho/godotenv"
	"github.com/yourusername/emailworker/internal/db"
	"github.com/yourusername/emailworker/internal/worker"
)

func main() {
	_ = godotenv.Load()

	ctx := context.Background()
	pool, err := db.Connect(ctx)
	if err != nil {
		log.Fatalf("FATAL DB connect: %v", err)
	}
	defer pool.Close()

	processor, err := worker.NewProcessorFromEnv(pool)
	if err != nil {
		log.Fatalf("FATAL worker config: %v", err)
	}

	if err := processor.Run(ctx); err != nil {
		log.Fatalf("FATAL worker: %v", err)
	}
}