package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// jwtMiddleware returns a Gin middleware (a handler that runs before the real handler).
// It validates the "Authorization: Bearer <token>" header on every protected route.
//
// In Go, functions can return other functions. This pattern is called a "closure".
// The outer function runs ONCE at startup. The returned inner function runs per-request.
func (s *Server) jwtMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. Extract the header value
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorResponse{
				Error: "Authorization header is required",
			})
			return
		}

		// strings.CutPrefix is like Python's str.removeprefix — it returns the
		// remaining string and whether the prefix was actually present.
		tokenString, found := strings.CutPrefix(authHeader, "Bearer ")
		if !found {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorResponse{
				Error: "Authorization header must start with 'Bearer '",
			})
			return
		}

		// 2. Parse + validate the JWT token.
		// The callback function receives the parsed (but unverified) token
		// and must return the secret key. This lets you inspect headers
		// first — here we reject anything that isn't HMAC-signed (prevents
		// the "alg:none" attack).
		token, err := jwt.Parse(tokenString, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, jwt.ErrSignatureInvalid
			}
			return s.jwtSecret, nil
		})

		if err != nil || !token.Valid {
			c.AbortWithStatusJSON(http.StatusUnauthorized, ErrorResponse{
				Error: "Invalid or expired token",
			})
			return
		}

		// 3. Stash the claims in Gin's context so handlers can read them.
		//    c.Set / c.Get is Gin's per-request key-value store.
		if claims, ok := token.Claims.(jwt.MapClaims); ok {
			c.Set("claims", claims)
			if sub, err := claims.GetSubject(); err == nil {
				c.Set("user_id", sub)
			}
		}

		// 4. c.Next() passes control to the next handler in the chain.
		//    Without this call, the real handler would never execute.
		c.Next()
	}
}
