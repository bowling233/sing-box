package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
)

func testOAuth(t *testing.T, secret string) *githubOAuth {
	t.Helper()
	oauth, err := newGitHubOAuth(option.APIGitHubOAuthOptions{Enabled: true, ClientID: "client", ClientSecret: "private", PublicURL: "https://dashboard.example", AllowedUserIDs: []int64{42}}, secret)
	if err != nil {
		t.Fatal(err)
	}
	return oauth
}

func TestGitHubOAuthConfiguration(t *testing.T) {
	optionsCanonical := testOAuth(t, "").options
	optionsCanonical.PublicURL = "https://DASHBOARD.example:443/"
	canonical, err := newGitHubOAuth(optionsCanonical, "")
	if err != nil || canonical.origin != "https://dashboard.example" {
		t.Fatal("public URL was not canonicalized")
	}

	for _, raw := range []string{"", "http://dashboard.example", "https://user:pass@dashboard.example", "https://dashboard.example/path", "https://dashboard.example?query=x", "https://dashboard.example#fragment"} {
		options := testOAuth(t, "").options
		options.PublicURL = raw
		if _, err := newGitHubOAuth(options, ""); err == nil {
			t.Errorf("accepted invalid public_url %q", raw)
		}
	}
	options := testOAuth(t, "").options
	options.AllowedUserIDs = nil
	if _, err := newGitHubOAuth(options, ""); err == nil {
		t.Error("empty allowlist accepted")
	}
	options.AllowedOrganizations = []string{"../foo"}
	if _, err := newGitHubOAuth(options, ""); err == nil {
		t.Error("invalid organization accepted")
	}
}

func TestGitHubOAuthStateAndPKCE(t *testing.T) {
	oauth := testOAuth(t, "")
	request := httptest.NewRequest("GET", "https://evil.example/auth/github/login", nil)
	request.Header.Set("X-Forwarded-Host", "evil.example")
	writer := httptest.NewRecorder()
	oauth.serveAuth(writer, request)
	redirect, _ := url.Parse(writer.Header().Get("Location"))
	values := redirect.Query()
	if values.Get("redirect_uri") != "https://dashboard.example/auth/github/callback" {
		t.Fatal("untrusted host influenced callback")
	}
	if values.Get("code_challenge_method") != "S256" {
		t.Fatal("missing PKCE S256")
	}
	cookie := writer.Result().Cookies()[0]
	if !cookie.Secure || !cookie.HttpOnly || cookie.Domain != "" || cookie.Path != "/" || cookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("unsafe cookie %+v", cookie)
	}
	state := oauth.states[cookie.Value]
	hash := sha256.Sum256([]byte(state.Verifier))
	if values.Get("code_challenge") != base64.RawURLEncoding.EncodeToString(hash[:]) {
		t.Fatal("incorrect PKCE challenge")
	}
	callback := httptest.NewRequest("GET", "https://dashboard.example/auth/github/callback?state="+cookie.Value, nil)
	if _, valid := oauth.consumeState(callback); valid {
		t.Fatal("accepted callback without browser cookie")
	}
	callback.AddCookie(cookie)
	if _, valid := oauth.consumeState(callback); !valid {
		t.Fatal("valid state rejected")
	}
	if _, valid := oauth.consumeState(callback); valid {
		t.Fatal("replayed state accepted")
	}
	oauth.states[cookie.Value] = oauthState{Expires: time.Now().Add(-time.Second)}
	if _, valid := oauth.consumeState(callback); valid {
		t.Fatal("expired state accepted")
	}
}

func TestGitHubOAuthAuthorization(t *testing.T) {
	oauth := testOAuth(t, "")
	oauth.sessions["session"] = &oauthSession{CSRF: "csrf", Expires: time.Now().Add(time.Hour)}
	cases := []struct {
		name, origin, csrf, authorization string
		cookie, allowed                   bool
	}{
		{name: "anonymous"},
		{name: "empty bearer", authorization: "Bearer "},
		{name: "generated secret cannot act as CLI secret", authorization: "Bearer " + oauth.internalSecret},
		{name: "cookie without csrf", origin: oauth.origin, cookie: true},
		{name: "cookie without origin", csrf: "csrf", cookie: true},
		{name: "cross origin", origin: "https://evil.example", csrf: "csrf", cookie: true},
		{name: "valid session", origin: oauth.origin, csrf: "csrf", cookie: true, allowed: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest("POST", "https://dashboard.example/rpc", nil)
			request.Header.Set("Origin", tc.origin)
			if tc.cookie {
				request.AddCookie(&http.Cookie{Name: oauthSessionCookie, Value: "session"})
			}
			// WebSocket metadata is separate from handshake headers, including forged Origin.
			headers := http.Header{"Origin": {oauth.origin}}
			headers.Set("X-CSRF-Token", tc.csrf)
			headers.Set("Authorization", tc.authorization)
			if got := oauth.authorizeHeaders(request, headers); got != tc.allowed {
				t.Fatalf("allowed=%v want %v", got, tc.allowed)
			}
			if tc.allowed && headers.Get("Authorization") != "Bearer "+oauth.internalSecret {
				t.Fatal("internal daemon credentials missing")
			}
		})
	}
	oauth.bearerSecret = "explicit-secret"
	request := httptest.NewRequest("POST", "https://dashboard.example/rpc", nil)
	request.Header.Set("Authorization", "Bearer explicit-secret")
	if !oauth.authorizeHeaders(request, request.Header) {
		t.Fatal("explicit CLI bearer rejected")
	}
	oauth.sessions["session"].Expires = time.Now().Add(-time.Second)
	request.AddCookie(&http.Cookie{Name: oauthSessionCookie, Value: "session"})
	if oauth.session(request) != nil {
		t.Fatal("expired session accepted")
	}
}

func TestGitHubOAuthAllowlist(t *testing.T) {
	var membershipState = "active"
	var membershipStatus = http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token" {
			t.Error("missing GitHub bearer")
		}
		switch r.URL.Path {
		case "/user":
			io.WriteString(w, `{"id":42,"login":"tester"}`)
		case "/user/memberships/orgs/example":
			w.WriteHeader(membershipStatus)
			json.NewEncoder(w).Encode(map[string]string{"state": membershipState})
		default:
			t.Errorf("unexpected URL %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	oauth := testOAuth(t, "")
	oauth.apiURL = server.URL
	if _, allowed, err := oauth.allowedUser(context.Background(), "token"); err != nil || !allowed {
		t.Fatalf("numeric ID rejected: %v", err)
	}
	oauth.options.AllowedUserIDs = []int64{99}
	oauth.options.AllowedOrganizations = []string{"example"}
	if _, allowed, err := oauth.allowedUser(context.Background(), "token"); err != nil || !allowed {
		t.Fatalf("active member rejected: %v", err)
	}
	membershipState = "pending"
	if _, allowed, _ := oauth.allowedUser(context.Background(), "token"); allowed {
		t.Fatal("pending member allowed")
	}
	membershipStatus = http.StatusForbidden
	if _, allowed, _ := oauth.allowedUser(context.Background(), "token"); allowed {
		t.Fatal("inaccessible membership allowed")
	}
	membershipStatus = http.StatusInternalServerError
	if _, allowed, err := oauth.allowedUser(context.Background(), "token"); allowed || err == nil {
		t.Fatal("GitHub error did not fail closed")
	}
}

func TestGitHubOAuthCallbackAndLogout(t *testing.T) {
	oauth := testOAuth(t, "")
	oauth.states["state"] = oauthState{Verifier: "verifier", Expires: time.Now().Add(time.Minute)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			r.ParseForm()
			if r.PostForm.Get("client_secret") != "private" || r.PostForm.Get("code_verifier") != "verifier" {
				t.Error("missing exchange credentials")
			}
			io.WriteString(w, `{"access_token":"github-private-token","token_type":"bearer"}`)
		case "/user":
			io.WriteString(w, `{"id":42,"login":"tester"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	oauth.tokenURL = server.URL + "/token"
	oauth.apiURL = server.URL
	request := httptest.NewRequest("GET", "https://dashboard.example/auth/github/callback?state=state&code=code", nil)
	request.AddCookie(&http.Cookie{Name: oauthStateCookie, Value: "state"})
	writer := httptest.NewRecorder()
	oauth.serveAuth(writer, request)
	if writer.Code != http.StatusFound || writer.Header().Get("Location") != "/dashboard/" {
		t.Fatalf("callback failed %d %s", writer.Code, writer.Body.String())
	}
	var sessionCookie *http.Cookie
	for _, cookie := range writer.Result().Cookies() {
		if cookie.Name == oauthSessionCookie {
			sessionCookie = cookie
		}
	}
	if sessionCookie == nil {
		t.Fatal("missing session cookie")
	}
	request = httptest.NewRequest("GET", "https://dashboard.example/auth/session", nil)
	request.AddCookie(sessionCookie)
	writer = httptest.NewRecorder()
	oauth.serveAuth(writer, request)
	if writer.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("session response cacheable")
	}
	if strings.Contains(writer.Body.String(), "private") {
		t.Fatal("credentials leaked to browser")
	}
	var response struct {
		CSRF string `json:"csrf_token"`
	}
	json.Unmarshal(writer.Body.Bytes(), &response)
	if response.CSRF == "" {
		t.Fatal("missing CSRF token")
	}
	request = httptest.NewRequest("POST", "https://dashboard.example/auth/logout", nil)
	request.AddCookie(sessionCookie)
	writer = httptest.NewRecorder()
	oauth.serveAuth(writer, request)
	if writer.Code != http.StatusForbidden {
		t.Fatal("logout without CSRF accepted")
	}
	request.Header.Set("Origin", oauth.origin)
	request.Header.Set("X-CSRF-Token", response.CSRF)
	writer = httptest.NewRecorder()
	oauth.serveAuth(writer, request)
	if writer.Code != http.StatusNoContent || oauth.session(request) != nil {
		t.Fatal("logout did not revoke session")
	}
}

func TestGitHubOAuthCapacity(t *testing.T) {
	oauth := testOAuth(t, "")
	for i := range oauthMaxEntries {
		oauth.states[string(rune(i))] = oauthState{Expires: time.Now().Add(time.Minute)}
	}
	writer := httptest.NewRecorder()
	oauth.login(writer, httptest.NewRequest("GET", "/auth/github/login", nil))
	if writer.Code != http.StatusTooManyRequests {
		t.Fatal("unbounded pending states")
	}
}

func TestGitHubOAuthBridgeTransports(t *testing.T) {
	oauth := testOAuth(t, "")
	oauth.sessions["session"] = &oauthSession{CSRF: "csrf", Expires: time.Now().Add(time.Hour)}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer "+oauth.internalSecret {
			return nil, status.Error(codes.Unauthenticated, "missing internal authentication")
		}
		return handler(ctx, req)
	}))
	defer grpcServer.Stop()
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	bridge := &webBridge{grpcServer: grpcServer, oauth: oauth}
	for _, contentType := range []string{contentTypeGRPC, contentTypeGRPCWeb} {
		for _, authenticated := range []bool{false, true} {
			request := httptest.NewRequest("POST", "https://dashboard.example/grpc.health.v1.Health/Check", bytes.NewReader([]byte{0, 0, 0, 0, 0}))
			request.ProtoMajor = 2
			request.ProtoMinor = 0
			request.Header.Set("Content-Type", contentType)
			if authenticated {
				request.AddCookie(&http.Cookie{Name: oauthSessionCookie, Value: "session"})
				request.Header.Set("Origin", oauth.origin)
				request.Header.Set("X-CSRF-Token", "csrf")
			}
			writer := httptest.NewRecorder()
			bridge.ServeHTTP(writer, request)
			if authenticated {
				if writer.Code != http.StatusOK || !bytes.Contains(writer.Body.Bytes(), []byte{0, 0, 0, 0, 2, 8, 1}) {
					t.Fatalf("%s authenticated call failed: code=%d body=%q", contentType, writer.Code, writer.Body.Bytes())
				}
			} else if writer.Code != http.StatusUnauthorized {
				t.Fatalf("%s anonymous call admitted", contentType)
			}
		}
	}
	server := httptest.NewServer(bridge)
	defer server.Close()
	for _, csrf := range []string{"csrf", "forged"} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		headers := http.Header{"Origin": {oauth.origin}, "Cookie": {oauthSessionCookie + "=session"}}
		conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/grpc.health.v1.Health/Check", &websocket.DialOptions{HTTPHeader: headers, Subprotocols: []string{webSocketSubprotocol}})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		err = conn.Write(ctx, websocket.MessageBinary, []byte("content-type: application/grpc-web+proto\r\nx-csrf-token: "+csrf+"\r\n"))
		if err != nil {
			conn.CloseNow()
			cancel()
			t.Fatal(err)
		}
		if csrf == "forged" {
			_, _, err = conn.Read(ctx)
			if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
				t.Errorf("forged WebSocket CSRF was not rejected: %v", err)
			}
		} else {
			conn.Write(ctx, websocket.MessageBinary, []byte{0, 0, 0, 0, 0, 0})
			conn.Write(ctx, websocket.MessageBinary, []byte{1})
			var received []byte
			for {
				_, frame, readErr := conn.Read(ctx)
				if readErr != nil {
					break
				}
				received = append(received, frame...)
			}
			if !bytes.Contains(received, []byte{0, 0, 0, 0, 2, 8, 1}) {
				t.Error("authenticated WebSocket health call failed")
			}
		}
		conn.CloseNow()
		cancel()
	}
}
