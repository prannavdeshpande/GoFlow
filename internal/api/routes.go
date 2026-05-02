package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server bundles all shared dependencies so handlers access them as methods.
// This is Go's idiomatic dependency injection — no global variables needed.
// The asterisk (*) means Server is a pointer type: all methods share one instance.
type Server struct {
	db        *pgxpool.Pool // PostgreSQL connection pool
	queue     *asynq.Client // Redis job queue client
	jwtSecret []byte        // HMAC secret for JWT validation
	router    *gin.Engine   // the Gin HTTP router
}

// NewServer wires up all dependencies and registers all routes.
// It returns a *Server (pointer), so callers share the same instance.
func NewServer(pool *pgxpool.Pool, queue *asynq.Client, jwtSecret []byte) *Server {
	s := &Server{
		db:        pool,
		queue:     queue,
		jwtSecret: jwtSecret,
		router:    gin.Default(),
	}
	s.registerRoutes()
	return s
}

// registerRoutes defines every URL path and which handler function it maps to.
func (s *Server) registerRoutes() {
	// Health check — no auth required.
	s.router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// All /jobs routes share the JWT middleware via a route Group.
	// Group("/jobs").Use(middleware) is equivalent to applying the middleware
	// individually to every route inside — just less repetition.
	jobs := s.router.Group("/jobs")
	jobs.Use(s.jwtMiddleware())
	{
		jobs.POST("", s.createJob)
		jobs.GET("", s.listJobs)
		jobs.GET("/:id", s.getJob)
		jobs.DELETE("/:id", s.deleteJob)
	}
}

// Run starts the HTTP server. It blocks until the server stops.
func (s *Server) Run(addr string) error {
	return s.router.Run(addr)
}
