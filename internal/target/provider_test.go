package target

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/railway"
)

var errBoom = errors.New("boom")

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeAPI is an in-memory implementation of railwayAPI. Behaviour is fully
// data-driven so table cases can wire up whatever topology / error they need.
type fakeAPI struct {
	// account-token surface
	me          *railway.Me
	meErr       error
	projects    []railway.Project
	projectsErr error

	// project(id) enrichment surface
	fullProjects map[string]*railway.Project
	projectErrs  map[string]error

	// project-token surface
	tokenInfo *railway.ProjectTokenInfo
	tokenErr  error

	// call counters (used by the cached tests)
	projectsCalls int
	projectCalls  int
}

func (f *fakeAPI) Me(ctx context.Context) (*railway.Me, error) { return f.me, f.meErr }

func (f *fakeAPI) Projects(ctx context.Context, workspaceID string) ([]railway.Project, error) {
	f.projectsCalls++
	if f.projectsErr != nil {
		return nil, f.projectsErr
	}
	return f.projects, nil
}

func (f *fakeAPI) Project(ctx context.Context, id string) (*railway.Project, error) {
	f.projectCalls++
	if err := f.projectErrs[id]; err != nil {
		return nil, err
	}
	if p, ok := f.fullProjects[id]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("fakeAPI: no project %q", id)
}

func (f *fakeAPI) ProjectToken(ctx context.Context) (*railway.ProjectTokenInfo, error) {
	return f.tokenInfo, f.tokenErr
}

// --- shared fixtures ---

func fullProject(id, name string, envs []railway.Environment, svcs []railway.Service) *railway.Project {
	return &railway.Project{ID: id, Name: name, Environments: envs, Services: svcs}
}

func acctCfg(projects, envs, svcs []string) *config.Config {
	return &config.Config{
		TokenType:    railway.TokenAccount,
		WorkspaceID:  "ws1",
		Projects:     projects,
		Environments: envs,
		Services:     svcs,
		DiscoveryTTL: time.Minute,
	}
}

func projCfg(projects, envs, svcs []string) *config.Config {
	c := acctCfg(projects, envs, svcs)
	c.TokenType = railway.TokenProject
	return c
}

// --- account provider ---

func TestAccountProvider(t *testing.T) {
	// A two-project workspace. p1 has two environments and two services; p2 has
	// a single environment. p3 exists in the listing but Project(id) errors.
	p1envs := []railway.Environment{{ID: "e1", Name: "prod"}, {ID: "e2", Name: "staging"}}
	p1svcs := []railway.Service{{ID: "s1", Name: "web"}, {ID: "s2", Name: "worker"}}
	p2envs := []railway.Environment{{ID: "e3", Name: "prod"}}
	p2svcs := []railway.Service{{ID: "s3", Name: "api"}}

	newFake := func() *fakeAPI {
		return &fakeAPI{
			projects: []railway.Project{
				{ID: "p1", Name: "P1"},
				{ID: "p2", Name: "P2"},
				{ID: "p3", Name: "P3"},
			},
			fullProjects: map[string]*railway.Project{
				"p1": fullProject("p1", "P1", p1envs, p1svcs),
				"p2": fullProject("p2", "P2", p2envs, p2svcs),
				"p3": fullProject("p3", "P3", nil, nil),
			},
			projectErrs: map[string]error{"p3": errBoom},
		}
	}

	allP1P2Svcs := []Service{{ID: "s1", Name: "web"}, {ID: "s2", Name: "worker"}}
	p2Svcs := []Service{{ID: "s3", Name: "api"}}

	tests := []struct {
		name     string
		projects []string
		envs     []string
		svcs     []string
		want     []Target
	}{
		{
			name: "enumerates one target per environment with services; errored project skipped",
			want: []Target{
				{ProjectID: "p1", ProjectName: "P1", EnvironmentID: "e1", EnvironmentName: "prod", Services: allP1P2Svcs},
				{ProjectID: "p1", ProjectName: "P1", EnvironmentID: "e2", EnvironmentName: "staging", Services: allP1P2Svcs},
				{ProjectID: "p2", ProjectName: "P2", EnvironmentID: "e3", EnvironmentName: "prod", Services: p2Svcs},
			},
		},
		{
			name:     "INTERMODAL_PROJECTS allowlist restricts to selected projects",
			projects: []string{"p2"},
			want: []Target{
				{ProjectID: "p2", ProjectName: "P2", EnvironmentID: "e3", EnvironmentName: "prod", Services: p2Svcs},
			},
		},
		{
			name: "INTERMODAL_ENVIRONMENTS allowlist restricts to selected environments",
			envs: []string{"e2", "e3"},
			want: []Target{
				{ProjectID: "p1", ProjectName: "P1", EnvironmentID: "e2", EnvironmentName: "staging", Services: allP1P2Svcs},
				{ProjectID: "p2", ProjectName: "P2", EnvironmentID: "e3", EnvironmentName: "prod", Services: p2Svcs},
			},
		},
		{
			name: "INTERMODAL_SERVICES allowlist restricts services within each target",
			svcs: []string{"s2"},
			want: []Target{
				{ProjectID: "p1", ProjectName: "P1", EnvironmentID: "e1", EnvironmentName: "prod", Services: []Service{{ID: "s2", Name: "worker"}}},
				{ProjectID: "p1", ProjectName: "P1", EnvironmentID: "e2", EnvironmentName: "staging", Services: []Service{{ID: "s2", Name: "worker"}}},
				// p2 has no s2 service -> empty service list, but the target still exists.
				{ProjectID: "p2", ProjectName: "P2", EnvironmentID: "e3", EnvironmentName: "prod"},
			},
		},
		{
			name:     "combined project + environment allowlist",
			projects: []string{"p1"},
			envs:     []string{"e1"},
			want: []Target{
				{ProjectID: "p1", ProjectName: "P1", EnvironmentID: "e1", EnvironmentName: "prod", Services: allP1P2Svcs},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFake()
			p := New(fake, acctCfg(tc.projects, tc.envs, tc.svcs), testLogger())
			got, err := p.Targets(context.Background())
			if err != nil {
				t.Fatalf("Targets() error = %v, want nil", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Targets() mismatch\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

func TestAccountProviderProjectsError(t *testing.T) {
	fake := &fakeAPI{projectsErr: errBoom}
	p := New(fake, acctCfg(nil, nil, nil), testLogger())
	got, err := p.Targets(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("Targets() error = %v, want errBoom", err)
	}
	if got != nil {
		t.Fatalf("Targets() = %+v, want nil", got)
	}
}

// --- project provider ---

func TestProjectProvider(t *testing.T) {
	envs := []railway.Environment{{ID: "e1", Name: "prod"}, {ID: "e2", Name: "staging"}}
	svcs := []railway.Service{{ID: "s1", Name: "web"}, {ID: "s2", Name: "worker"}}

	tests := []struct {
		name        string
		tokenInfo   *railway.ProjectTokenInfo
		full        map[string]*railway.Project
		projectErrs map[string]error
		cfgEnvs     []string
		cfgSvcs     []string
		want        []Target
		wantErr     bool
	}{
		{
			name:      "enrichment returns only the scoped environment with services",
			tokenInfo: &railway.ProjectTokenInfo{ProjectID: "p1", EnvironmentID: "e2"},
			full:      map[string]*railway.Project{"p1": fullProject("p1", "P1", envs, svcs)},
			want: []Target{
				{ProjectID: "p1", ProjectName: "P1", EnvironmentID: "e2", EnvironmentName: "staging",
					Services: []Service{{ID: "s1", Name: "web"}, {ID: "s2", Name: "worker"}}},
			},
		},
		{
			name:      "enrichment respects the service allowlist",
			tokenInfo: &railway.ProjectTokenInfo{ProjectID: "p1", EnvironmentID: "e1"},
			full:      map[string]*railway.Project{"p1": fullProject("p1", "P1", envs, svcs)},
			cfgSvcs:   []string{"s1"},
			want: []Target{
				{ProjectID: "p1", ProjectName: "P1", EnvironmentID: "e1", EnvironmentName: "prod",
					Services: []Service{{ID: "s1", Name: "web"}}},
			},
		},
		{
			name:        "Project(id) error falls back to a bare target (IDs only)",
			tokenInfo:   &railway.ProjectTokenInfo{ProjectID: "p1", EnvironmentID: "e1"},
			projectErrs: map[string]error{"p1": errBoom},
			want:        []Target{{ProjectID: "p1", EnvironmentID: "e1"}},
		},
		{
			name:        "Project(id) error with environment filtered out yields nothing",
			tokenInfo:   &railway.ProjectTokenInfo{ProjectID: "p1", EnvironmentID: "e1"},
			projectErrs: map[string]error{"p1": errBoom},
			cfgEnvs:     []string{"e9"},
			want:        nil,
		},
		{
			name:      "enrichment succeeds but scoped env filtered out -> falls through to filtered bare target (nil)",
			tokenInfo: &railway.ProjectTokenInfo{ProjectID: "p1", EnvironmentID: "e1"},
			full:      map[string]*railway.Project{"p1": fullProject("p1", "P1", envs, svcs)},
			cfgEnvs:   []string{"e9"},
			want:      nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeAPI{
				tokenInfo:    tc.tokenInfo,
				fullProjects: tc.full,
				projectErrs:  tc.projectErrs,
			}
			p := New(fake, projCfg(nil, tc.cfgEnvs, tc.cfgSvcs), testLogger())
			got, err := p.Targets(context.Background())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("Targets() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Targets() error = %v, want nil", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Targets() mismatch\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

func TestProjectProviderTokenError(t *testing.T) {
	fake := &fakeAPI{tokenErr: errBoom}
	p := New(fake, projCfg(nil, nil, nil), testLogger())
	if _, err := p.Targets(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Targets() error = %v, want errBoom", err)
	}
}

// --- cached wrapper ---

func TestCached(t *testing.T) {
	full := fullProject("p1", "P1", []railway.Environment{{ID: "e1", Name: "prod"}}, nil)
	fake := &fakeAPI{
		projects:     []railway.Project{{ID: "p1", Name: "P1"}},
		fullProjects: map[string]*railway.Project{"p1": full},
	}

	prov := New(fake, acctCfg(nil, nil, nil), testLogger())
	c, ok := prov.(*cached)
	if !ok {
		t.Fatalf("New() returned %T, want *cached", prov)
	}
	ctx := context.Background()

	// 1. Initial fetch populates the cache.
	got1, err := c.Targets(ctx)
	if err != nil {
		t.Fatalf("initial Targets() error = %v", err)
	}
	want1 := []Target{{ProjectID: "p1", ProjectName: "P1", EnvironmentID: "e1", EnvironmentName: "prod"}}
	if !reflect.DeepEqual(got1, want1) {
		t.Fatalf("initial Targets() = %+v, want %+v", got1, want1)
	}
	if fake.projectsCalls != 1 {
		t.Fatalf("after initial fetch projectsCalls = %d, want 1", fake.projectsCalls)
	}

	// 2. Within the TTL the cache is served without re-fetching.
	if _, err := c.Targets(ctx); err != nil {
		t.Fatalf("cached Targets() error = %v", err)
	}
	if fake.projectsCalls != 1 {
		t.Fatalf("within TTL projectsCalls = %d, want 1 (no refetch)", fake.projectsCalls)
	}

	// 3. Expire the cache and make the fetch fail: stale data is served, no error.
	fake.projectsErr = errBoom
	c.expiry = time.Now().Add(-time.Hour)
	got3, err := c.Targets(ctx)
	if err != nil {
		t.Fatalf("stale Targets() error = %v, want nil (should serve stale)", err)
	}
	if !reflect.DeepEqual(got3, want1) {
		t.Fatalf("stale Targets() = %+v, want cached %+v", got3, want1)
	}
	if fake.projectsCalls != 2 {
		t.Fatalf("after expired failing fetch projectsCalls = %d, want 2", fake.projectsCalls)
	}

	// 4. Expire again and let the fetch recover with new data: cache refreshes.
	fake.projectsErr = nil
	full.Environments = []railway.Environment{{ID: "e2", Name: "staging"}}
	c.expiry = time.Now().Add(-time.Hour)
	got4, err := c.Targets(ctx)
	if err != nil {
		t.Fatalf("refresh Targets() error = %v", err)
	}
	want4 := []Target{{ProjectID: "p1", ProjectName: "P1", EnvironmentID: "e2", EnvironmentName: "staging"}}
	if !reflect.DeepEqual(got4, want4) {
		t.Fatalf("refreshed Targets() = %+v, want %+v", got4, want4)
	}
	if fake.projectsCalls != 3 {
		t.Fatalf("after refresh projectsCalls = %d, want 3", fake.projectsCalls)
	}
}

func TestCachedFetchErrorWithoutCache(t *testing.T) {
	fake := &fakeAPI{projectsErr: errBoom}
	prov := New(fake, acctCfg(nil, nil, nil), testLogger())
	if _, err := prov.Targets(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Targets() error = %v, want errBoom (no cache to fall back on)", err)
	}
}

// --- Target.ServiceName ---

func TestTargetServiceName(t *testing.T) {
	tgt := Target{Services: []Service{{ID: "s1", Name: "web"}, {ID: "s2", Name: ""}}}
	tests := []struct {
		id, want string
	}{
		{"s1", "web"},          // known, named
		{"s2", "s2"},           // known but unnamed -> falls back to ID
		{"unknown", "unknown"}, // absent -> ID
	}
	for _, tc := range tests {
		if got := tgt.ServiceName(tc.id); got != tc.want {
			t.Errorf("ServiceName(%q) = %q, want %q", tc.id, got, tc.want)
		}
	}
}
