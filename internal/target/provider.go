// Package target turns the account-vs-project token distinction into a single
// uniform list of scrape/stream targets. Everything downstream (metrics poller,
// log pipeline) consumes []Target and never has to know which token type is in
// play.
package target

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/railway"
)

// Service is a service within a target environment.
type Service struct {
	ID   string
	Name string
	// DeploymentID is the active (SUCCESS) deployment in this environment, used
	// to stream HTTP/edge logs; empty when there is no active deployment.
	DeploymentID string
	// HasDomain reports whether the service has a public domain — a prerequisite
	// for HTTP logs.
	HasDomain bool
}

// Target is one Railway environment plus the services it contains. It is the
// natural unit for both metrics (one `metrics` call per environment, grouped by
// service) and logs (one environmentLogs subscription per environment).
type Target struct {
	ProjectID       string
	ProjectName     string
	EnvironmentID   string
	EnvironmentName string
	Services        []Service
}

// ServiceName returns the human name for a service ID within this target, or
// the ID itself if unknown.
func (t Target) ServiceName(id string) string {
	for _, s := range t.Services {
		if s.ID == id {
			if s.Name != "" {
				return s.Name
			}
			break
		}
	}
	return id
}

// Provider yields the current set of targets. Implementations cache with a TTL.
type Provider interface {
	Targets(ctx context.Context) ([]Target, error)
}

// railwayAPI is the subset of *railway.Client that discovery needs. Declaring
// it here keeps the providers unit-testable with a fake.
type railwayAPI interface {
	Me(ctx context.Context) (*railway.Me, error)
	Projects(ctx context.Context, workspaceID string) ([]railway.Project, error)
	Project(ctx context.Context, id string) (*railway.Project, error)
	ProjectToken(ctx context.Context) (*railway.ProjectTokenInfo, error)
}

// New returns the provider appropriate to the configured token type.
func New(api railwayAPI, cfg *config.Config, log *slog.Logger) Provider {
	if log == nil {
		log = slog.Default()
	}
	filt := filters{
		projects:     toSet(cfg.Projects),
		environments: toSet(cfg.Environments),
		services:     toSet(cfg.Services),
	}
	var fetch fetchFunc
	if cfg.TokenType == railway.TokenProject {
		p := &projectProvider{api: api, filt: filt, log: log}
		fetch = p.fetch
	} else {
		p := &accountProvider{api: api, workspaceID: cfg.WorkspaceID, filt: filt, log: log}
		fetch = p.fetch
	}
	return &cached{ttl: cfg.DiscoveryTTL, fetch: fetch, log: log}
}

type fetchFunc func(ctx context.Context) ([]Target, error)

// cached wraps a fetch with a TTL and serves stale data if a refresh fails, so
// a transient Railway hiccup doesn't blank out all targets. It never holds the
// lock across the network fetch: the poller and log manager share one instance,
// and singleflight coalesces their concurrent refreshes into a single call.
type cached struct {
	ttl   time.Duration
	fetch fetchFunc
	log   *slog.Logger
	group singleflight.Group

	mu     sync.Mutex
	val    []Target
	expiry time.Time
}

func (c *cached) Targets(ctx context.Context) ([]Target, error) {
	c.mu.Lock()
	if c.val != nil && time.Now().Before(c.expiry) {
		v := c.val
		c.mu.Unlock()
		return v, nil
	}
	c.mu.Unlock()

	ch := c.group.DoChan("targets", func() (any, error) {
		val, err := c.fetch(ctx)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.val = val
		c.expiry = time.Now().Add(c.ttl)
		c.mu.Unlock()
		return val, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			c.mu.Lock()
			stale := c.val
			c.mu.Unlock()
			if stale != nil {
				c.log.Warn("discovery refresh failed; serving cached targets", "err", res.Err, "targets", len(stale))
				return stale, nil
			}
			return nil, res.Err
		}
		return res.Val.([]Target), nil
	}
}

// --- account/workspace token provider ---

type accountProvider struct {
	api         railwayAPI
	workspaceID string
	filt        filters
	log         *slog.Logger
}

func (p *accountProvider) fetch(ctx context.Context) ([]Target, error) {
	projects, err := p.api.Projects(ctx, p.workspaceID)
	if err != nil {
		return nil, err
	}
	var targets []Target
	for _, proj := range projects {
		if !p.filt.allowProject(proj.ID, proj.Name) {
			continue
		}
		full, err := p.api.Project(ctx, proj.ID)
		if err != nil {
			// One inaccessible project shouldn't sink the whole refresh.
			p.log.Warn("skipping project during discovery", "project_id", proj.ID, "err", err)
			continue
		}
		targets = append(targets, p.filt.buildTargets(full)...)
	}
	return targets, nil
}

// --- project token provider ---

type projectProvider struct {
	api  railwayAPI
	filt filters
	log  *slog.Logger
}

func (p *projectProvider) fetch(ctx context.Context) ([]Target, error) {
	info, err := p.api.ProjectToken(ctx)
	if err != nil {
		return nil, err
	}
	// Best-effort enrichment: a project token may not be allowed to read
	// project(id). If it fails, fall back to a bare target with just IDs — the
	// metrics/log paths only need the environment ID.
	if full, err := p.api.Project(ctx, info.ProjectID); err == nil {
		for _, t := range p.filt.buildTargets(full) {
			if t.EnvironmentID == info.EnvironmentID {
				return []Target{t}, nil
			}
		}
	} else {
		p.log.Debug("project(id) enrichment failed for project token; using bare target", "err", err)
	}
	if !p.filt.allowEnvironment(info.EnvironmentID, "") {
		return nil, nil
	}
	return []Target{{ProjectID: info.ProjectID, EnvironmentID: info.EnvironmentID}}, nil
}

// --- filtering ---

type filters struct {
	projects     map[string]struct{}
	environments map[string]struct{}
	services     map[string]struct{}
}

func (f filters) allowProject(id, name string) bool     { return allow(f.projects, id, name) }
func (f filters) allowEnvironment(id, name string) bool { return allow(f.environments, id, name) }
func (f filters) allowService(id, name string) bool     { return allow(f.services, id, name) }

// buildTargets expands a project into one Target per (allowed) environment,
// attaching its (allowed) services.
func (f filters) buildTargets(p *railway.Project) []Target {
	// Index service instances by service+environment for deployment/domain lookup.
	inst := make(map[string]railway.ServiceInstance, len(p.Instances))
	for _, si := range p.Instances {
		inst[si.ServiceID+"|"+si.EnvironmentID] = si
	}
	var targets []Target
	for _, env := range p.Environments {
		if !f.allowEnvironment(env.ID, env.Name) {
			continue
		}
		var services []Service
		for _, s := range p.Services {
			if !f.allowService(s.ID, s.Name) {
				continue
			}
			svc := Service{ID: s.ID, Name: s.Name}
			if si, ok := inst[s.ID+"|"+env.ID]; ok {
				svc.HasDomain = si.HasDomain
				if si.DeploymentStatus == "SUCCESS" {
					svc.DeploymentID = si.DeploymentID
				}
			}
			services = append(services, svc)
		}
		targets = append(targets, Target{
			ProjectID:       p.ID,
			ProjectName:     p.Name,
			EnvironmentID:   env.ID,
			EnvironmentName: env.Name,
			Services:        services,
		})
	}
	return targets
}

// toSet builds a case-insensitive membership set. Entries may be IDs or names.
func toSet(items []string) map[string]struct{} {
	if len(items) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(items))
	for _, it := range items {
		m[strings.ToLower(it)] = struct{}{}
	}
	return m
}

// allow returns true if the set is empty (no filter) or contains the resource's
// id or name (case-insensitive) — so allowlists accept either.
func allow(set map[string]struct{}, id, name string) bool {
	if len(set) == 0 {
		return true
	}
	if id != "" {
		if _, ok := set[strings.ToLower(id)]; ok {
			return true
		}
	}
	if name != "" {
		if _, ok := set[strings.ToLower(name)]; ok {
			return true
		}
	}
	return false
}
