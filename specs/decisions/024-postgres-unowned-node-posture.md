# ADR-024: An unowned node's PostgreSQL front-end stays bound and refuses by name

> Date: 2026-10-22
> Status: Accepted
> Related: ADR-006 (auth-first schema), ADR-010 (production mode),
> `design-cql-role-auth-rollout.md`

## Context

A node with `auth_enabled=false` binds the PostgreSQL listener and cannot
authenticate anybody. Measured on 2026-10-22 against such a node:

```text
$ psql "host=127.0.0.1 port=5432 user=ferrosa_admin"
psql: error: connection to server at "127.0.0.1", port 5432 failed: FATAL:  Handshake(UnknownRole)
node log: PostgreSQL connection refused during startup error=Handshake(UnknownRole)
```

The mechanism is not a bug in the wire layer. `ferrosa-postgres` resolves a
role's SCRAM verifier before it offers SASL (`handshake::VerifierStore::verifier`
→ `Schema::scram_credential`), and the node skips **both** `seed_default_roles`
and the `system_auth` replay when auth is disabled. The registry therefore holds
no role with a verifier, `verifier(user)` is `None` for every user, and every
login is answered with an internal enum name that names neither the cause nor
the next step.

Two consequences worth stating plainly:

1. "Every PostgreSQL client can query the box" is not a default posture of the
   node. It is a consequence of CQL auth being **on** (which is what seeds
   roles).
2. Production is already protected: `enforce_production_requirements` refuses
   `FERROSA_MODE=production` with auth disabled. This ADR is about the
   non-production default posture — the one every development node and every
   pre-owner appliance image boots into.

Options considered:

- **(a) Refuse by name** — keep the listener bound, refuse every login with a
  human-actionable `FATAL`, and say it once at startup.
- **(b) Do not bind** — refuse to start the PostgreSQL listener while
  `auth_enabled=false` (or while no role has a verifier), with a startup log
  line naming the key that turns it on.
- **(c) Password-less `trust` mode** — rejected outright; see below.

## Decision

**Option (a). The listener stays bound; every login on a node with no login
role is refused by name, once per connection at the wire, once at startup in the
log.**

### What a client sees

One line, SQLSTATE `28000`

<!-- markdownlint-disable-next-line MD013 -->
```text
FATAL:  no roles are configured on this node, so no login can authenticate: enable CQL auth (FERROSA_AUTH_ENABLED=true) to seed the login roles, then set FERROSA_SEED_ADMIN_PASSWORD to choose the administrator password
```

`28000` (`invalid_authorization_specification`) is deliberate: `28P01`
(`invalid_password`) would claim a password was tried against a stored
verifier, and there is no verifier to try. A `psql` client renders it verbatim:

```text
psql: error: connection to server at "127.0.0.1", port 5432 failed: FATAL:  no roles are configured on this node, so no login can authenticate: enable CQL auth (FERROSA_AUTH_ENABLED=true) to seed the login roles, then set FERROSA_SEED_ADMIN_PASSWORD to choose the administrator password
```

A driver sees the same text in its error object and `28000` in its SQLSTATE
(tokio-postgres: `DbError::message` / `DbError::code`).

### How the node decides

- `SchemaVerifierStore::login_roles_configured()` — any role on the node with
  `can_login` **and** a stored SCRAM verifier. The **registry** is the source of
  truth, not the config flag: the registry is what decides whether a login can
  succeed, and a node whose registry really does hold a login role (in-process
  tests, a node whose roles arrived some other way) can authenticate — saying
  otherwise would be a lie the client can disprove.
- `Handshake::on_startup` returns the new `HandshakeError::NoRolesConfigured`
  when the named role has no verifier **and** the node has no login role at all.
  `UnknownRole` remains the answer on a node that has login roles, where it
  still counts as a failed login.
- `NoRolesConfigured` is **not** recorded against the failed-login limiter:
  there is no role to lock out, and charging the limiter would let an unowned
  node's refusals pollute a real role's backoff state.
- The node logs the same text **once**, at the point it builds the listener's
  store, with the bind address:
  `PostgreSQL listener is bound and cannot authenticate anyone: <text>`. The
  per-connection refusal is logged at `INFO` as it always was; the actionable
  text is not repeated per connection.
- `seed_default_roles` itself is untouched: `ferrosa_admin`/`ferrosa_admin`
  stays development-only, with the 5-minute "rotate it" warning.

### Why (a) and not (b), "do not bind"

- **A closed port is not an explanation.** `ECONNREFUSED` is indistinguishable
  from a crashed node, the wrong host, or the wrong port. The operator is
  standing at the client; that is where the reason belongs.
- **The listener is truthful.** The node is running, `5432` is open by
  configuration, and the refusal is a policy fact. Reporting that fact is more
  honest than making the port look like an accident.
- **No second mechanism.** Refusing to bind would need readiness/healthcheck and
  port-map semantics to explain itself, and would make "is the front-end
  enabled?" a question the client cannot answer.
- **"A server that cannot serve is a failure, not a mode" is satisfied.** The
  failure is reported, by name, to the client and to the operator once at
  startup. Silent degradation is what that principle forbids, not a named
  refusal.

Option (b) remains available on the roadmap if a future decision wants the
front-end off for unowned nodes — but it must then also change the appliance
image (which enables auth and seeds an administrator) and this record.

### Why (c), password-less `trust`, is rejected

An appliance database that answers on loopback without a credential is the same
class of mistake this board just spent a day removing. Loopback is not a
boundary here: the appliance forwards `5432`, and any process on the box is
already a user. No password-less path is added, not even behind a development
flag. The fix for "no credential works" is to create a credential, not to delete
authentication.

## Consequences

- **Appliance path unchanged and still satisfiable.** With CQL auth on and
  `ferrosa_admin` seeded, the same listener authenticates and serves
  `SELECT 1`; the live test in `ferrosa-postgres/tests/pg_no_roles_posture.rs`
  pins exactly that, on the same listener instance that refused a moment
  earlier (no restart).
- **Development dogfooding unchanged.** A node with auth disabled still binds
  `5432`; it still refuses logins (nothing that worked stops working), and now
  says why.
- **One new warning on every auth-disabled boot** (the development default).
  That is the point: the posture is degraded and visible rather than disguised as
  a client error.
- **Enumeration.** `NoRolesConfigured` is not a new oracle: on such a node every
  role is unknown anyway, and the message describes the node, not the user. The
  existing note that `UnknownRole` is an oracle on a *provisioned* node stands.
- **Tests.** Unit: `handshake::tests::node_without_login_roles_is_refused_by_name`,
  `node_with_roles_still_reports_an_unknown_role`,
  `store::tests::{fresh_schema_has_no_login_role,
  a_role_with_a_verifier_is_a_configured_login_role}`. Live (real driver over
  loopback, message asserted by name):
  `pg_no_roles_posture::{a_node_with_no_roles_refuses_every_login_by_name,
  the_same_listener_serves_select_1_once_a_role_exists}`.
- **Not in this record:** a *provisioned* node still answers
  `Handshake(UnknownRole)` and `Handshake(Scram(ProofMismatch))` — client errors
  against a server that can serve. Naming those is a separate change (it touches
  the user-enumeration trade-off); this record is only about the node that cannot
  serve at all.

## Verification (measured, 2026-10-22)

A debug node built from this branch, booted with `FERROSA_AUTH_ENABLED=false`
(`FERROSA_POSTGRES_BIND=127.0.0.1:18442`, fresh data directory):

```text
$ psql "host=127.0.0.1 port=18442 user=ferrosa_admin dbname=ferrosa" -c 'select 1'
psql (PostgreSQL) 18.6
psql: error: connection to server at "127.0.0.1", port 18442 failed: FATAL:  no roles are configured on this node, so no login can authenticate: enable CQL auth (FERROSA_AUTH_ENABLED=true) to seed the login roles, then set FERROSA_SEED_ADMIN_PASSWORD to choose the administrator password
```

with `user=nobody` the same message: the refusal is about the node, not about
who asked. In the node's own log it appears once, with the bind address:

```text
WARN ferrosa: PostgreSQL listener is bound and cannot authenticate anyone: no roles are
configured on this node, so no login can authenticate: enable CQL auth
(FERROSA_AUTH_ENABLED=true) to seed the login roles, then set
FERROSA_SEED_ADMIN_PASSWORD to choose the administrator password
bind=127.0.0.1:18442
```

A second node booted from the same build with `FERROSA_AUTH_ENABLED=true` and
`FERROSA_SEED_ADMIN_PASSWORD` seeds the roles, emits no posture warning, and the
same `psql` command serves the row the appliance needs:

```text
$ PGPASSWORD=… psql "host=127.0.0.1 port=18443 user=ferrosa_admin dbname=ferrosa" -c 'SELECT 1 AS one'
 one
-----
   1
(1 row)
```

On that provisioned node the pre-existing refusals are unchanged — measured on
the same build, `psql` against `user=ghost` and a wrong password for
`ferrosa_admin`:

```text
FATAL:  Handshake(UnknownRole)
FATAL:  Handshake(Scram(ProofMismatch))
```

An unknown role is still charged as a failed login (`AuthFailed` in the audit
log); only the unowned-node refusal is exempt, because there is no role to lock
out and charging the limiter there would pollute a real role's backoff state.
