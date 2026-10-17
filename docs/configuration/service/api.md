---
icon: material/new-box
---

!!! question "Since sing-box 1.14.0"

# sing-box API

The sing-box API service is a gRPC server for observing and controlling the running sing-box instance.

It can be accessed by the [sing-box graphical clients](/clients/) for iOS, macOS, and
Android (via the Remote Control feature), or the
[sing-box dashboard](https://github.com/SagerNet/sing-box-dashboard).

The server also accepts [gRPC-Web](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md) requests.

### Structure

```json
{
  "type": "api",
  
  ... // Listen Fields
  
  "secret": "",
  "access_control_allow_origin": [],
  "access_control_allow_private_network": false,
  "dashboard": {
    "enabled": true,
    "path": "",
    "download_url": "",
    "http_client": "", // or {}
    "update_interval": ""
  },
  "tls": {}
}
```

### Listen Fields

See [Listen Fields](/configuration/shared/listen/) for details.

### Fields

#### secret

Secret for the API.

Clients authenticate with the standard `authorization: Bearer <secret>` gRPC metadata header.

If empty, authentication is disabled unless `github_oauth.enabled` is true.
With GitHub OAuth enabled, an empty secret disables bearer authentication only.

#### access_control_allow_origin

CORS allowed origins, `*` will be used if empty.

#### access_control_allow_private_network

Allow access from private network.

#### dashboard

Web dashboard downloaded and served over the API listener at `/dashboard/`; other browser
requests are redirected to it.

!!! info ""

    The object can be replaced with a boolean value (equivalent to `{ "enabled": <bool> }`),
    or with a string path (equivalent to `{ "enabled": true, "path": "<string>" }`).

##### enabled

Enable the dashboard.

##### path

Directory the dashboard files are stored in.

`dashboard` in the working directory will be used by default.

If the directory contains files not downloaded by sing-box, they are served as-is.

##### download_url

Download URL of the dashboard archive (zip).

`https://github.com/SagerNet/sing-box-dashboard/archive/refs/heads/gh-pages.zip` will be used by default.

##### http_client

HTTP client used to download the dashboard.

See [HTTP Client Fields](/configuration/shared/http-client/) for details.

##### update_interval

Update interval of the dashboard.

`1d` will be used by default.

#### tls

TLS configuration, see [TLS](/configuration/shared/tls/#inbound).

#### github_oauth

GitHub OAuth authentication for the dashboard served by this API. This fork's dashboard
supports automatically selecting the same-origin backend and bypassing the manual login form.
Use `dashboard.download_url` pointing to
`https://github.com/bowling233/sing-box-dashboard/archive/refs/heads/gh-pages.zip`.

```json
{
  "github_oauth": {
    "enabled": true,
    "client_id": "YOUR_GITHUB_OAUTH_APP_CLIENT_ID",
    "client_secret": "YOUR_GITHUB_OAUTH_APP_CLIENT_SECRET",
    "public_url": "https://dashboard.example.com",
    "allowed_user_ids": [12345678],
    "allowed_organizations": ["example-org"],
    "http_client": "proxy-download"
  }
}
```

Register a GitHub OAuth App with the authorization callback URL
`https://dashboard.example.com/auth/github/callback`. `public_url` must be the externally
accessible HTTPS origin, without a path, query, or fragment. TLS may terminate at a reverse
proxy. Forward `/auth/`, `/dashboard/`, and all API paths to the same listener, preserving
`Cookie`, `Origin`, and `X-CSRF-Token` headers. GitHub credentials stay on the server.

`allowed_user_ids` contains stable, positive GitHub numeric user IDs, not usernames.
`allowed_organizations` contains organization slugs. A user is admitted if their ID matches
**or** they are an **active** member of any listed organization. At least one allowlist entry
is required; an empty list never permits everyone. Organization checks request the `read:org`
scope and require the organization to permit the OAuth App. Pending or inaccessible memberships
do not grant access. Memberships and allowlists are checked at sign-in, not continuously.

`http_client` selects the outbound HTTP client used for GitHub token and API requests; see
[HTTP Client Fields](/configuration/shared/http-client/). If omitted, a transport with default HTTP client options is created;
`route.default_http_client` is not selected automatically. Set this field explicitly when
GitHub requests need a particular proxy or detour.

Unauthenticated dashboard requests redirect to GitHub. Authentication uses host-only,
Secure, HttpOnly, SameSite=Lax cookies, one-time state, and PKCE S256. Sessions expire after
eight hours; sign-in attempts expire after five minutes. Sessions and pending sign-ins are
stored in memory, bounded to 1024 each, and cleared when the service restarts. Use a single
replica or session affinity. Logout revokes future requests; already-established gRPC streams
continue until they disconnect. Removing access from a running session requires a restart
or waiting for expiry, and established streams must also be closed.

When OAuth is enabled, an empty `secret` disables bearer authentication rather than disabling
authentication. A nonempty `secret` continues to allow CLI/native clients with
`authorization: Bearer <secret>`. Browser sessions require the same-origin `Origin` header and
the session's `X-CSRF-Token` for all gRPC transports, including native gRPC, gRPC-Web, and
WebSockets. The CSRF token is returned by `GET /auth/session`; no API secret or GitHub access
token is sent to the browser. `POST /auth/logout` requires the same session, Origin, and CSRF
token. Cross-origin hosted dashboards are not supported in OAuth mode.
