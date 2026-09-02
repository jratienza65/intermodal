package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/railway"
	"github.com/jratienza65/intermodal/internal/target"
)

// errStopTrial stops a trial subscription after the first record.
var errStopTrial = errors.New("stop trial")

// doctor probes the configured token and reports its capabilities, so the
// project-vs-account token question (and any auth/scope issue) is answered
// concretely before running for real. It prints a human report to stdout.
func doctor(ctx context.Context, cfg *config.Config, client *railway.Client, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	fmt.Println("intermodal doctor")
	fmt.Println("=================")
	fmt.Printf("token type:      %s\n", cfg.TokenType.String())
	fmt.Printf("http endpoint:   %s\n", firstOr(cfg.HTTPEndpoints, railway.DefaultHTTPEndpoints[0]))
	fmt.Printf("ws endpoint:     %s\n", firstOr(cfg.WSEndpoints, railway.DefaultWSEndpoints[0]))
	fmt.Printf("instance id:     %s\n", nameOr(cfg.Identity.InstanceID, "(none — self-metrics from several instances will collide; set INTERMODAL_INSTANCE_ID)"))
	fmt.Println()

	// 1. Identity (account tokens only).
	if cfg.TokenType == railway.TokenAccount {
		if me, err := client.Me(ctx); err != nil {
			fmt.Printf("identity:        FAILED (%v)\n", err)
		} else {
			fmt.Printf("identity:        %s <%s>\n", me.Name, me.Email)
		}
	} else {
		if info, err := client.ProjectToken(ctx); err != nil {
			fmt.Printf("project scope:   FAILED (%v)\n", err)
		} else {
			fmt.Printf("project scope:   project=%s environment=%s\n", info.ProjectID, info.EnvironmentID)
		}
	}

	// 2. Discovery.
	provider := target.New(client, cfg, logger)
	targets, err := provider.Targets(ctx)
	if err != nil {
		fmt.Printf("discovery:       FAILED (%v)\n", err)
		return nil
	}
	fmt.Printf("discovery:       %d target environment(s)\n", len(targets))
	for _, t := range targets {
		fmt.Printf("  - %s / %s (env=%s, %d service(s))\n",
			nameOr(t.ProjectName, t.ProjectID), nameOr(t.EnvironmentName, t.EnvironmentID),
			t.EnvironmentID, len(t.Services))
	}
	if len(targets) == 0 {
		fmt.Println("\nNo targets — nothing to probe. Check token scope and allowlists.")
		return nil
	}

	first := targets[0]

	// 3. Trial metrics call.
	results, err := client.Metrics(ctx, railway.MetricsQuery{
		Measurements:      railway.DefaultMeasurements,
		ProjectID:         first.ProjectID,
		EnvironmentID:     first.EnvironmentID,
		StartDate:         time.Now().Add(-15 * time.Minute),
		GroupBy:           []railway.MetricTag{railway.TagServiceID},
		SampleRateSeconds: 60,
	})
	if err != nil {
		fmt.Printf("metrics probe:   FAILED (%v)\n", err)
	} else {
		fmt.Printf("metrics probe:   OK (%d series for env %s)\n", len(results), first.EnvironmentID)
	}

	// 4. Trial log subscription.
	n, err := trialSubscribe(ctx, client, first.EnvironmentID)
	switch {
	case err != nil:
		fmt.Printf("log probe:       FAILED (%v)\n", err)
	case n > 0:
		fmt.Printf("log probe:       OK (received %d log line(s))\n", n)
	default:
		fmt.Printf("log probe:       OK (connected; no logs within the probe window)\n")
	}

	fmt.Println("\nDone.")
	return nil
}

// trialSubscribe opens a short-lived subscription and returns how many lines it
// saw. A clean timeout with no error means the subscription connected fine.
func trialSubscribe(ctx context.Context, client *railway.Client, envID string) (int, error) {
	sctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	count := 0
	err := client.StreamEnvironmentLogs(sctx, railway.LogStreamParams{
		EnvironmentID: envID,
		BeforeLimit:   1,
	}, func(railway.LogEntry) error {
		count++
		return errStopTrial
	})
	switch {
	case errors.Is(err, errStopTrial):
		return count, nil
	case err == nil, errors.Is(err, context.DeadlineExceeded):
		return count, nil
	default:
		return count, err
	}
}

func firstOr(list []string, def string) string {
	if len(list) > 0 {
		return list[0]
	}
	return def
}

func nameOr(name, id string) string {
	if name != "" {
		return name
	}
	return id
}
