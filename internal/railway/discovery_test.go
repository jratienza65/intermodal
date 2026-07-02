package railway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// routeServer dispatches GraphQL POSTs by operationName to the given handlers,
// each returning the JSON for the `data` field.
func routeServer(t *testing.T, handlers map[string]func(vars map[string]any) string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			OperationName string         `json:"operationName"`
			Variables     map[string]any `json:"variables"`
		}
		json.Unmarshal(body, &req)
		h, ok := handlers[req.OperationName]
		if !ok {
			t.Errorf("unexpected operation %q", req.OperationName)
			w.Write([]byte(`{"errors":[{"message":"unexpected op"}]}`))
			return
		}
		w.Write([]byte(`{"data":` + h(req.Variables) + `}`))
	}))
}

func TestProjectsPagination(t *testing.T) {
	srv := routeServer(t, map[string]func(map[string]any) string{
		"intermodalProjects": func(vars map[string]any) string {
			if vars["after"] == nil {
				return `{"projects":{"edges":[{"node":{"id":"p1","name":"one"}},{"node":{"id":"p2","name":"two"}}],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}}}`
			}
			return `{"projects":{"edges":[{"node":{"id":"p3","name":"three"}}],"pageInfo":{"hasNextPage":false,"endCursor":""}}}`
		},
	})
	defer srv.Close()

	c := fastClient(Auth{Token: "t", Type: TokenAccount}, srv.URL)
	projects, err := c.Projects(context.Background(), "")
	if err != nil {
		t.Fatalf("Projects: %v", err)
	}
	if len(projects) != 3 {
		t.Fatalf("expected 3 projects across pages, got %d (%+v)", len(projects), projects)
	}
	if projects[2].ID != "p3" || projects[2].Name != "three" {
		t.Errorf("last project = %+v", projects[2])
	}
}

func TestProjectMapsEnvironmentsAndServices(t *testing.T) {
	srv := routeServer(t, map[string]func(map[string]any) string{
		"intermodalProject": func(vars map[string]any) string {
			if vars["id"] != "p1" {
				t.Errorf("project id var = %v", vars["id"])
			}
			return `{"project":{"id":"p1","name":"one",
				"environments":{"edges":[{"node":{"id":"e1","name":"production"}},{"node":{"id":"e2","name":"staging"}}]},
				"services":{"edges":[{"node":{"id":"s1","name":"api"}}]}}}`
		},
	})
	defer srv.Close()

	c := fastClient(Auth{Token: "t", Type: TokenAccount}, srv.URL)
	p, err := c.Project(context.Background(), "p1")
	if err != nil {
		t.Fatalf("Project: %v", err)
	}
	if len(p.Environments) != 2 || p.Environments[0].Name != "production" {
		t.Errorf("environments = %+v", p.Environments)
	}
	if len(p.Services) != 1 || p.Services[0].ID != "s1" {
		t.Errorf("services = %+v", p.Services)
	}
}

func TestProjectTokenAndMe(t *testing.T) {
	srv := routeServer(t, map[string]func(map[string]any) string{
		"intermodalProjectToken": func(map[string]any) string {
			return `{"projectToken":{"projectId":"pX","environmentId":"eX"}}`
		},
		"intermodalMe": func(map[string]any) string {
			return `{"me":{"id":"u1","name":"Ada","email":"ada@example.com"}}`
		},
	})
	defer srv.Close()

	c := fastClient(Auth{Token: "t", Type: TokenProject}, srv.URL)
	info, err := c.ProjectToken(context.Background())
	if err != nil {
		t.Fatalf("ProjectToken: %v", err)
	}
	if info.ProjectID != "pX" || info.EnvironmentID != "eX" {
		t.Errorf("projectToken = %+v", info)
	}

	me, err := c.Me(context.Background())
	if err != nil {
		t.Fatalf("Me: %v", err)
	}
	if me.Email != "ada@example.com" {
		t.Errorf("me = %+v", me)
	}
}
