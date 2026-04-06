# Bug: SPARQL Health Endpoint Ignores Auth

**Severity:** Low
**Branch:** feat/sparql-endpoint
**File:** ferrosa-sparql/src/http.rs:140-145

## Issue

`GET /sparql/health` returns 200 OK unconditionally, even when `auth_disabled = false`. Reveals service is running to unauthenticated clients.

## Fix

Check `AppState.auth_disabled` flag. Return 401 if auth is required and no credentials provided.

## Estimated Effort

5 minutes.
