# M1 backlog: deferred review findings

These are minor findings from the M1 per-task reviews and the final review, deferred during the build. None blocks merge. Findings already fixed later in M1 are left out.

## First M2 task
- ~~**Reconciler.** Re-attach control-plane, worker and the shared helpers to `label=sijil.tenant` networks after compose recreates them. Today a `make up` that rebuilds those services silently cuts active tenants off.~~ Done in M2: `sijil_worker.reconcile`, run when the worker starts and every 300 s by `beat`.
- ~~**IP pool for dev tenant networks.** Use a dedicated small IPAM pool; Docker's default pools allow about 31 bridge networks.~~ Done in M2: tenant networks are /28s from `SIJIL_TENANT_SUBNET_POOL` (`10.240.0.0/16`).

## Production milestone
- Set uvicorn `forwarded_allow_ips` to the real proxy, so rate limits key on the client IP and not the proxy's.
- Refuse to start with `SIJIL_COOKIE_SECURE=false` unless `environment=dev`.
- Keep the `ALTER ROLE … PASSWORD` statement out of the Postgres server log (`log_min_error_statement`).
- Detect drift between a running tenant container and its spec (image, env) instead of reusing it silently.
- Add a reaper or outbox for provisioning jobs whose Celery hand-off was lost after commit.

## Security and robustness
- **Owner changes.** Two concurrent changes can lock rows in opposite order, deadlock, and return a 500. Lock the owner set or the org row first.
- **Duplicate provisioning runs.** Concurrent runs with different service passwords can race; add a per-job lock or compare-and-set. `_store_token` can raise an `IntegrityError` on a first-time race (it self-heals on retry).
- **TOTP limiter.** It is per user, so anyone holding the password can lock the owner out of TOTP and enrolment for 15 minutes. Consider a per-(user, IP) counter plus a higher per-user ceiling.
- **Argon2 work per IP.** Login limits per (email, IP), so cycling emails gives unbounded hashing per IP. Add a per-IP ceiling.
- **Slug collision.** A collision raises an unhandled `IntegrityError`; add one retry.
- **`require_workspace`.** It should depend on `require_totp_compliance` itself, so it is safe by default; today the router-level dependency covers it.
- **Legacy dev tenants.** Tenants created before opaque ids have slug-derived database names, which `destroy(ref)` won't find.
- **`compose_project` length.** It is only validated at provisioning time; check it at worker startup.
- **Migrations.** The Alembic engine lacks `hide_parameters=True`.

## API and error shape
- `POST /v1/workspaces` authorises with `org_manage_members`.
- `OrgPatch.region` shows as settable in OpenAPI.
- Verify on a full session without TOTP returns 401 `auth.totp_invalid`; 403 or 409 would describe it better.
- A password over 4096 characters gets the generic validation code instead of `auth.weak_password`.
- Signup commits the user, then the session, separately; a failure in between returns 500 but leaves the account.
- `DELETE` of your own current session leaves the cookie.
- The `IntegrityError` mapping matches a substring, not `constraint_name`.
- There is no argon2 rehash when parameters change.
- A decrypt failure returns 500 after counting an attempt (untested).
- A non-JSON 200 from Paperless raises an unmapped `KeyError` or `JSONDecodeError`.
- An upload retried on 502–504 may queue a duplicate task; add a comment.
- `wait_ready` treats a proxy's 403 as "ready".
- An unhandled exception's traceback is logged twice, and the 500 envelope lacks middleware headers.

## Crypto
- `DataKey`'s repr shows the plaintext data key; use `field(repr=False)`.
- The master-key validator rejects a trailing newline.
- `to_bytes` raises a raw `struct.error` for a `key_id` of 65,536 bytes or more.
- Document that `derive()` must not be called with the reserved `sijil:kek:` prefix.
- Document that `open_envelope` trusts `env.key_id`, so callers must bind the workspace in `context`.

## Tests and tooling
- **Root `conftest.py`** isn't covered by mypy.
- **Migrations.** There is no migration-versus-model drift test, and enum columns lack DB CHECK constraints.
- **Pure tests** (`test_email_is_normalised`, `test_role_rank`) carry the `db` marker.
- **Ruff.** The S105 ignore covers all of `models.py`.
- **`Role.rank`** is keyed by string literals.
- **Fixtures.** Concurrent test runs can race on `CREATE ROLE`. Test clients don't run the lifespan, so engines and Redis pools aren't disposed; `make_app` is called 52 times. A missing `SIJIL_TEST_REDIS_URL` gives a bare `KeyError`.
- **App lifecycle.** Lifespan cleanup isn't in `try/finally`. The CSRF path check ignores `root_path`.
- **CSRF tests** don't cover an empty header or cookie, PUT/PATCH/DELETE, the GET bypass, or response bodies.
- **Untested paths:**
  - the TOTP replay guard under a real race;
  - an enveloped `totp_pending`;
  - `CeleryEnqueuer`'s `send_task` arguments;
  - the member-role R3 case;
  - an admin promoting themselves;
  - the unknown-country error code;
  - Paperless client backoff values, `ConnectError` exhaustion, 404, `task()` parsing, `upload`;
  - nonce truncation.
- **Test precision.** One test uses `raises(Exception)`, and the stale pending-session test doesn't check its error code.
- **Integration tests** leave documents, users and `active` rows behind. An empty password default makes them wait 180 s before failing. Cross-tenant checks test DNS, not routing by IP. Smoke's negative database logins pass on any connection failure. `make_spec` duplicates `tenant_db_names`.
- **`Dockerfile.tests`** has no `.dockerignore`, and the read-write `/src` mount leaves root-owned caches on Linux.
- **Compose.** The Postgres password is interpolated into URLs without encoding. `tessdata-init` hardcodes the 3.4.1 image instead of `SIJIL_TENANT_IMAGE`.
- **Housekeeping.**
  - Duplicated `Clock`.
  - A private SQLAlchemy typing import.
  - `keep_expiry` reads `expires_at` after commit.
  - A dead `step` initial value.
  - The failure log lacks the exception type.
  - A `print` in the acceptance test.
  - A redundant `GRANT CONNECT` on `postgres`.
  - Stale tenant-network wording in CLAUDE.md.
