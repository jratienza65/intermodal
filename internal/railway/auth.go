package railway

import "net/http"

// TokenType selects the auth header scheme. Account and workspace tokens share
// the Bearer scheme; project tokens use a dedicated header. Mixing them yields
// "Not Authorized" from Railway, so this must be chosen correctly up front.
type TokenType int

const (
	// TokenAccount covers both personal (account) and workspace (team) tokens,
	// which authenticate identically via `Authorization: Bearer`.
	TokenAccount TokenType = iota
	// TokenProject is a project-scoped token using the Project-Access-Token header.
	TokenProject
)

func (t TokenType) String() string {
	switch t {
	case TokenProject:
		return "project"
	default:
		return "account"
	}
}

// Auth carries a Railway API token and how to present it.
type Auth struct {
	Token string
	Type  TokenType
}

// apply sets the correct auth header on an outgoing request.
func (a Auth) apply(h http.Header) {
	switch a.Type {
	case TokenProject:
		h.Set("Project-Access-Token", a.Token)
	default:
		h.Set("Authorization", "Bearer "+a.Token)
	}
}

// connectionInitPayload builds the graphql-ws connection_init payload. Browsers
// can't set WebSocket headers, so Railway (like every graphql-ws server) accepts
// auth in this payload. We also set the header on the dial request as a belt-and-
// suspenders measure for non-browser clients.
func (a Auth) connectionInitPayload() map[string]any {
	switch a.Type {
	case TokenProject:
		return map[string]any{"Project-Access-Token": a.Token}
	default:
		return map[string]any{"Authorization": "Bearer " + a.Token}
	}
}
