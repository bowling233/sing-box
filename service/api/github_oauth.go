package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/option"
)

const (
	oauthSessionCookie   = "__Host-sing-box-session"
	oauthStateCookie     = "__Host-sing-box-oauth-state"
	oauthStateLifetime   = 5 * time.Minute
	oauthSessionLifetime = 8 * time.Hour
	oauthMaxEntries      = 1024
)

type githubUser struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
}

type oauthSession struct {
	User    githubUser
	CSRF    string
	Expires time.Time
}

type oauthState struct {
	Verifier string
	Expires  time.Time
}

type githubOAuth struct {
	options        option.APIGitHubOAuthOptions
	origin         string
	internalSecret string
	bearerSecret   string
	client         *http.Client
	authorizeURL   string
	tokenURL       string
	apiURL         string
	mu             sync.Mutex
	states         map[string]oauthState
	sessions       map[string]*oauthSession
}

func newGitHubOAuth(options option.APIGitHubOAuthOptions, secret string) (*githubOAuth, error) {
	u, err := url.Parse(options.PublicURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("public_url must be an HTTPS origin without a path, query, or fragment")
	}
	if options.ClientID == "" || options.ClientSecret == "" {
		return nil, errors.New("client_id and client_secret are required")
	}
	if len(options.AllowedUserIDs) == 0 && len(options.AllowedOrganizations) == 0 {
		return nil, errors.New("at least one allowed_user_ids or allowed_organizations entry is required")
	}
	for _, id := range options.AllowedUserIDs {
		if id <= 0 {
			return nil, errors.New("allowed_user_ids must contain positive GitHub numeric IDs")
		}
	}
	for _, org := range options.AllowedOrganizations {
		if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]{0,38}$`).MatchString(org) {
			return nil, errors.New("allowed_organizations must contain GitHub organization slugs")
		}
	}
	host := strings.ToLower(u.Hostname())
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if u.Port() != "" && u.Port() != "443" {
		host = net.JoinHostPort(strings.ToLower(u.Hostname()), u.Port())
	}
	originalSecret := secret
	if secret == "" {
		secret = randomOAuthToken()
	}
	return &githubOAuth{
		options: options, origin: u.Scheme + "://" + host, internalSecret: secret, bearerSecret: originalSecret,
		client:       &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		authorizeURL: "https://github.com/login/oauth/authorize", tokenURL: "https://github.com/login/oauth/access_token", apiURL: "https://api.github.com",
		states: make(map[string]oauthState), sessions: make(map[string]*oauthSession),
	}, nil
}

func randomOAuthToken() string {
	var value [32]byte
	// crypto/rand.Read always fills its buffer or terminates the process.
	rand.Read(value[:])
	return base64.RawURLEncoding.EncodeToString(value[:])
}

func oauthCookie(name, value string, lifetime time.Duration) *http.Cookie {
	return &http.Cookie{Name: name, Value: value, Path: "/", Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: int(lifetime.Seconds())}
}

func (o *githubOAuth) pruneLocked() {
	now := time.Now()
	for key, state := range o.states {
		if !now.Before(state.Expires) {
			delete(o.states, key)
		}
	}
	for key, session := range o.sessions {
		if !now.Before(session.Expires) {
			delete(o.sessions, key)
		}
	}
}

func (o *githubOAuth) session(request *http.Request) *oauthSession {
	cookie, err := request.Cookie(oauthSessionCookie)
	if err != nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	session := o.sessions[cookie.Value]
	if session != nil && !time.Now().Before(session.Expires) {
		delete(o.sessions, cookie.Value)
		return nil
	}
	return session
}

// authorizeHeaders uses the HTTP handshake's Origin and cookie, never websocket
// metadata, for browser identity. Only the CSRF token comes from gRPC metadata.
func (o *githubOAuth) authorizeHeaders(request *http.Request, headers http.Header) bool {
	if o.optionsSecretMatches(headers.Get("Authorization")) {
		return true
	}
	session := o.session(request)
	if session == nil || request.Header.Get("Origin") != o.origin || !constantEqual(headers.Get("X-CSRF-Token"), session.CSRF) {
		return false
	}
	headers.Set("Authorization", "Bearer "+o.internalSecret)
	return true
}

func (o *githubOAuth) optionsSecretMatches(header string) bool {
	// Only an explicitly configured API secret enables bearer authentication.
	return o.bearerSecret != "" && constantEqual(header, "Bearer "+o.bearerSecret)
}

func constantEqual(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

func (o *githubOAuth) authorize(writer http.ResponseWriter, request *http.Request, headers http.Header) bool {
	if o.authorizeHeaders(request, headers) {
		return true
	}
	writer.Header().Set("Cache-Control", "no-store")
	http.Error(writer, "authentication required", http.StatusUnauthorized)
	return false
}

func (o *githubOAuth) serveAuth(writer http.ResponseWriter, request *http.Request) bool {
	if !strings.HasPrefix(request.URL.Path, "/auth/") {
		return false
	}
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Referrer-Policy", "no-referrer")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	switch request.URL.Path {
	case "/auth/session":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			break
		}
		writer.Header().Set("Content-Type", "application/json")
		session := o.session(request)
		if session == nil {
			json.NewEncoder(writer).Encode(map[string]any{"enabled": true, "authenticated": false})
			break
		}
		json.NewEncoder(writer).Encode(map[string]any{"enabled": true, "authenticated": true, "csrf_token": session.CSRF, "user": session.User})
	case "/auth/github/login":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			break
		}
		o.login(writer, request)
	case "/auth/github/callback":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			break
		}
		o.callback(writer, request)
	case "/auth/logout":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			break
		}
		session := o.session(request)
		if session == nil || request.Header.Get("Origin") != o.origin || !constantEqual(request.Header.Get("X-CSRF-Token"), session.CSRF) {
			http.Error(writer, "invalid session or CSRF token", http.StatusForbidden)
			break
		}
		cookie, _ := request.Cookie(oauthSessionCookie)
		o.mu.Lock()
		delete(o.sessions, cookie.Value)
		o.mu.Unlock()
		http.SetCookie(writer, oauthCookie(oauthSessionCookie, "", -time.Second))
		writer.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(writer, request)
	}
	return true
}

func (o *githubOAuth) login(writer http.ResponseWriter, request *http.Request) {
	state := randomOAuthToken()
	verifier := randomOAuthToken()
	o.mu.Lock()
	o.pruneLocked()
	// Starting a new flow in the same browser invalidates its previous one.
	if old, err := request.Cookie(oauthStateCookie); err == nil {
		delete(o.states, old.Value)
	}
	if len(o.states) >= oauthMaxEntries {
		o.mu.Unlock()
		http.Error(writer, "too many sign-in attempts; try again later", http.StatusTooManyRequests)
		return
	}
	o.states[state] = oauthState{Verifier: verifier, Expires: time.Now().Add(oauthStateLifetime)}
	o.mu.Unlock()
	challenge := sha256.Sum256([]byte(verifier))
	query := url.Values{"client_id": {o.options.ClientID}, "redirect_uri": {o.origin + "/auth/github/callback"}, "state": {state}, "code_challenge": {base64.RawURLEncoding.EncodeToString(challenge[:])}, "code_challenge_method": {"S256"}}
	if len(o.options.AllowedOrganizations) > 0 {
		query.Set("scope", "read:org")
	}
	http.SetCookie(writer, oauthCookie(oauthStateCookie, state, oauthStateLifetime))
	http.Redirect(writer, request, o.authorizeURL+"?"+query.Encode(), http.StatusFound)
}

func (o *githubOAuth) consumeState(request *http.Request) (oauthState, bool) {
	stateKey := request.URL.Query().Get("state")
	cookie, err := request.Cookie(oauthStateCookie)
	if err != nil || stateKey == "" || !constantEqual(stateKey, cookie.Value) {
		return oauthState{}, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	state, found := o.states[stateKey]
	delete(o.states, stateKey)
	return state, found && time.Now().Before(state.Expires)
}

func (o *githubOAuth) callback(writer http.ResponseWriter, request *http.Request) {
	state, valid := o.consumeState(request)
	http.SetCookie(writer, oauthCookie(oauthStateCookie, "", -time.Second))
	if !valid || request.URL.Query().Get("code") == "" || request.URL.Query().Get("error") != "" {
		http.Error(writer, "invalid or expired GitHub authorization; start sign-in again", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 30*time.Second)
	defer cancel()
	token, err := o.exchangeCode(ctx, request.URL.Query().Get("code"), state.Verifier)
	if err != nil {
		http.Error(writer, "GitHub authorization failed", http.StatusBadGateway)
		return
	}
	user, allowed, err := o.allowedUser(ctx, token)
	if err != nil {
		http.Error(writer, "unable to verify GitHub access", http.StatusBadGateway)
		return
	}
	if !allowed {
		http.Error(writer, "this GitHub account is not allowed", http.StatusForbidden)
		return
	}
	sessionID := randomOAuthToken()
	session := &oauthSession{User: user, CSRF: randomOAuthToken(), Expires: time.Now().Add(oauthSessionLifetime)}
	o.mu.Lock()
	o.pruneLocked()
	if len(o.sessions) >= oauthMaxEntries {
		o.mu.Unlock()
		http.Error(writer, "too many active sessions; try again later", http.StatusServiceUnavailable)
		return
	}
	if old, err := request.Cookie(oauthSessionCookie); err == nil {
		delete(o.sessions, old.Value)
	}
	o.sessions[sessionID] = session
	o.mu.Unlock()
	http.SetCookie(writer, oauthCookie(oauthSessionCookie, sessionID, oauthSessionLifetime))
	http.Redirect(writer, request, dashboardRoutePrefix, http.StatusFound)
}

func (o *githubOAuth) exchangeCode(ctx context.Context, code, verifier string) (string, error) {
	values := url.Values{"client_id": {o.options.ClientID}, "client_secret": {o.options.ClientSecret}, "code": {code}, "redirect_uri": {o.origin + "/auth/github/callback"}, "code_verifier": {verifier}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, o.tokenURL, strings.NewReader(values.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")
	response, err := o.client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GitHub token status: %d", response.StatusCode)
	}
	var result struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		Error       string `json:"error"`
	}
	err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result)
	if err != nil {
		return "", err
	}
	if result.Error != "" || result.AccessToken == "" || !strings.EqualFold(result.TokenType, "bearer") {
		return "", errors.New("invalid GitHub token response")
	}
	return result.AccessToken, nil
}

func (o *githubOAuth) apiGet(ctx context.Context, token, path string, target any) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, o.apiURL+path, nil)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("User-Agent", "sing-box-dashboard")
	response, err := o.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return response.StatusCode, nil
	}
	return response.StatusCode, json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(target)
}

func (o *githubOAuth) allowedUser(ctx context.Context, token string) (githubUser, bool, error) {
	var user githubUser
	status, err := o.apiGet(ctx, token, "/user", &user)
	if err != nil {
		return user, false, err
	}
	if status != http.StatusOK || user.ID <= 0 || user.Login == "" {
		return user, false, errors.New("invalid GitHub user")
	}
	for _, id := range o.options.AllowedUserIDs {
		if user.ID == id {
			return user, true, nil
		}
	}
	for _, org := range o.options.AllowedOrganizations {
		var membership struct {
			State string `json:"state"`
		}
		status, err = o.apiGet(ctx, token, "/user/memberships/orgs/"+url.PathEscape(org), &membership)
		if err != nil {
			return user, false, err
		}
		if status == http.StatusOK && membership.State == "active" {
			return user, true, nil
		}
		if status != http.StatusOK && status != http.StatusNotFound && status != http.StatusForbidden {
			return user, false, errors.New("GitHub organization lookup failed")
		}
	}
	return user, false, nil
}
