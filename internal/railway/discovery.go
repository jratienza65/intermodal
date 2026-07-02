package railway

import "context"

// Me returns the authenticated user. Works only with account tokens; workspace
// and project tokens return an error.
func (c *Client) Me(ctx context.Context) (*Me, error) {
	const q = `query intermodalMe { me { id name email } }`
	var data struct {
		Me Me `json:"me"`
	}
	if err := c.Execute(ctx, Request{OpName: "intermodalMe", Query: q}, &data); err != nil {
		return nil, err
	}
	return &data.Me, nil
}

// ProjectToken returns the project/environment a project token is scoped to.
func (c *Client) ProjectToken(ctx context.Context) (*ProjectTokenInfo, error) {
	const q = `query intermodalProjectToken { projectToken { projectId environmentId } }`
	var data struct {
		ProjectToken ProjectTokenInfo `json:"projectToken"`
	}
	if err := c.Execute(ctx, Request{OpName: "intermodalProjectToken", Query: q}, &data); err != nil {
		return nil, err
	}
	return &data.ProjectToken, nil
}

// idName is the common {id,name} node shape.
type idName struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type projectsConnection struct {
	Edges []struct {
		Node idName `json:"node"`
	} `json:"edges"`
	PageInfo struct {
		HasNextPage bool   `json:"hasNextPage"`
		EndCursor   string `json:"endCursor"`
	} `json:"pageInfo"`
}

// Projects lists projects visible to the token, following Relay pagination.
// Pass a non-empty workspaceID to scope to a specific workspace; an empty
// string uses the account-wide default.
func (c *Client) Projects(ctx context.Context, workspaceID string) ([]Project, error) {
	const q = `query intermodalProjects($first: Int, $after: String, $workspaceId: String) {
  projects(first: $first, after: $after, workspaceId: $workspaceId) {
    edges { node { id name } }
    pageInfo { hasNextPage endCursor }
  }
}`
	var projects []Project
	var after string
	for {
		vars := map[string]any{"first": 100}
		if after != "" {
			vars["after"] = after
		}
		if workspaceID != "" {
			vars["workspaceId"] = workspaceID
		}
		var data struct {
			Projects projectsConnection `json:"projects"`
		}
		if err := c.Execute(ctx, Request{OpName: "intermodalProjects", Query: q, Variables: vars}, &data); err != nil {
			return nil, err
		}
		for _, e := range data.Projects.Edges {
			projects = append(projects, Project{ID: e.Node.ID, Name: e.Node.Name})
		}
		if !data.Projects.PageInfo.HasNextPage || data.Projects.PageInfo.EndCursor == "" {
			break
		}
		after = data.Projects.PageInfo.EndCursor
	}
	return projects, nil
}

// Project returns a single project with its environments and services.
func (c *Client) Project(ctx context.Context, id string) (*Project, error) {
	// Request an explicit large page for the nested connections so behavior
	// doesn't depend on an unknown server-side default page cap; warn (below) if
	// a project ever exceeds it rather than silently dropping environments.
	const q = `query intermodalProject($id: String!) {
  project(id: $id) {
    id
    name
    environments(first: 500) { edges { node { id name } } pageInfo { hasNextPage } }
    services(first: 500) {
      edges { node {
        id
        name
        serviceInstances {
          edges { node {
            environmentId
            latestDeployment { id status }
            domains { serviceDomains { domain } customDomains { domain } }
          } }
        }
      } }
      pageInfo { hasNextPage }
    }
  }
}`
	type envEdges struct {
		Edges []struct {
			Node idName `json:"node"`
		} `json:"edges"`
		PageInfo struct {
			HasNextPage bool `json:"hasNextPage"`
		} `json:"pageInfo"`
	}
	type domain struct {
		Domain string `json:"domain"`
	}
	var data struct {
		Project struct {
			ID           string   `json:"id"`
			Name         string   `json:"name"`
			Environments envEdges `json:"environments"`
			Services     struct {
				Edges []struct {
					Node struct {
						ID               string `json:"id"`
						Name             string `json:"name"`
						ServiceInstances struct {
							Edges []struct {
								Node struct {
									EnvironmentID    string `json:"environmentId"`
									LatestDeployment *struct {
										ID     string `json:"id"`
										Status string `json:"status"`
									} `json:"latestDeployment"`
									Domains struct {
										ServiceDomains []domain `json:"serviceDomains"`
										CustomDomains  []domain `json:"customDomains"`
									} `json:"domains"`
								} `json:"node"`
							} `json:"edges"`
						} `json:"serviceInstances"`
					} `json:"node"`
				} `json:"edges"`
				PageInfo struct {
					HasNextPage bool `json:"hasNextPage"`
				} `json:"pageInfo"`
			} `json:"services"`
		} `json:"project"`
	}
	if err := c.Execute(ctx, Request{OpName: "intermodalProject", Query: q, Variables: map[string]any{"id": id}}, &data); err != nil {
		return nil, err
	}
	if data.Project.Environments.PageInfo.HasNextPage || data.Project.Services.PageInfo.HasNextPage {
		c.log.Warn("project has >500 environments or services; some may be unmonitored", "project_id", id)
	}
	p := &Project{ID: data.Project.ID, Name: data.Project.Name}
	for _, e := range data.Project.Environments.Edges {
		p.Environments = append(p.Environments, Environment{ID: e.Node.ID, Name: e.Node.Name})
	}
	for _, se := range data.Project.Services.Edges {
		sn := se.Node
		p.Services = append(p.Services, Service{ID: sn.ID, Name: sn.Name})
		for _, ie := range sn.ServiceInstances.Edges {
			in := ie.Node
			si := ServiceInstance{
				ServiceID:     sn.ID,
				EnvironmentID: in.EnvironmentID,
				HasDomain:     len(in.Domains.ServiceDomains) > 0 || len(in.Domains.CustomDomains) > 0,
			}
			if in.LatestDeployment != nil {
				si.DeploymentID = in.LatestDeployment.ID
				si.DeploymentStatus = in.LatestDeployment.Status
			}
			p.Instances = append(p.Instances, si)
		}
	}
	return p, nil
}
