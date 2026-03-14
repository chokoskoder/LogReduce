package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"
)

func main() {
	f, err := os.Create("app_logs.json")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	logger := slog.New(slog.NewJSONHandler(f, nil))

	services := []string{
		"auth-service",
		"payment-service",
		"user-service",
		"inventory-service",
		"notification-service",
		"search-service",
	}

	// endpoints grouped by service for realism
	serviceEndpoints := map[string][]string{
		"auth-service":         {"/login", "/logout", "/refresh", "/register"},
		"payment-service":      {"/charge", "/refund", "/webhook", "/balance"},
		"user-service":         {"/profile", "/settings", "/avatar", "/preferences"},
		"inventory-service":    {"/stock", "/reserve", "/release", "/catalog"},
		"notification-service": {"/send", "/subscribe", "/unsubscribe", "/status"},
		"search-service":       {"/query", "/suggest", "/index", "/health"},
	}

	// base latency per endpoint type — slow endpoints are consistently slow
	baseLatency := map[string]int{
		"/login": 200, "/logout": 50, "/refresh": 80, "/register": 300,
		"/charge": 400, "/refund": 350, "/webhook": 100, "/balance": 60,
		"/profile": 70, "/settings": 90, "/avatar": 150, "/preferences": 60,
		"/stock": 80, "/reserve": 120, "/release": 60, "/catalog": 200,
		"/send": 250, "/subscribe": 100, "/unsubscribe": 50, "/status": 30,
		"/query": 180, "/suggest": 90, "/index": 500, "/health": 10,
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	totalEntries := 1_000_000

	start := time.Now()
	for i := 0; i < totalEntries; i++ {
		svc := services[r.Intn(len(services))]
		endpoints := serviceEndpoints[svc]
		endpoint := endpoints[r.Intn(len(endpoints))]

		// status weighted: ~85% success, ~8% client error, ~7% server error
		statusRoll := r.Intn(100)
		var status int
		switch {
		case statusRoll < 85:
			status = 200
		case statusRoll < 88:
			status = 201
		case statusRoll < 90:
			status = 400
		case statusRoll < 92:
			status = 401
		case statusRoll < 93:
			status = 403
		case statusRoll < 95:
			status = 404
		case statusRoll < 97:
			status = 500
		case statusRoll < 98:
			status = 502
		case statusRoll < 99:
			status = 503
		default:
			status = 504
		}

		// latency: base + random variance + occasional spikes
		base := baseLatency[endpoint]
		latency := base + r.Intn(base/2+1)
		if r.Intn(100) < 3 { // 3% chance of a latency spike
			latency = latency * (3 + r.Intn(5))
		}

		level := slog.LevelInfo
		msg := "request processed"
		if status >= 500 {
			level = slog.LevelError
			msg = "internal server error"
		} else if status >= 400 {
			level = slog.LevelWarn
			msg = "client error"
		}

		logger.Log(context.Background(), level, msg,
			slog.String("service", svc),
			slog.String("path", endpoint),
			slog.Int("status", status),
			slog.Int("latency_ms", latency),
			slog.Int("user_id", r.Intn(10000)),
		)

		if (i+1)%100000 == 0 {
			fmt.Printf("generated %d / %d entries\n", i+1, totalEntries)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("done. %d entries in %s\n", totalEntries, elapsed)
}