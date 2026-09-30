# Campus Ledger API

The demo serves same-origin JSON on `http://127.0.0.1:4192`. All writes are
serialized through one local Rust database handle. Start it with
`cargo run --locked -p university-demo -- serve --data ./data/university`.

| Method | Path | Meaning |
| --- | --- | --- |
| GET | `/api/health` | Runtime, public log info and project counts; not an integrity audit |
| GET | `/api/projects?search=energy&status=active&course=ENV-402` | Current matching projects |
| GET | `/api/projects/{id}` | One current project |
| POST | `/api/projects` | Append creation; 201 on success |
| PATCH | `/api/projects/{id}` | Append revision-checked edit |
| POST | `/api/projects/{id}/archive` | Append final archive event |
| GET | `/api/projects/{id}/history` | Complete project event history |
| GET | `/api/events` | Latest 50 events, newest first |
| POST | `/api/audit` | Authenticate signed head and available block bytes |
| GET | `/api/export` | Public signed proof bundle download |

Example creation:

```sh
curl http://127.0.0.1:4192/api/projects \
  -H 'Content-Type: application/json' \
  -d '{"id":"energy-study","title":"Campus energy study","summary":"Fictional teaching project","course":"ENV-402","supervisor":"Demo supervisor","team":["Demo student"],"tags":["energy"],"actor":"Demo operator"}'
```

New projects start `planned`, revision 1. `actor` is a plain audit label, not an
authenticated identity. The server supplies UTC timestamps. IDs are immutable
1–64 ASCII letters, digits, hyphens or underscores.

```sh
curl -X PATCH http://127.0.0.1:4192/api/projects/energy-study \
  -H 'Content-Type: application/json' \
  -d '{"expected_version":1,"actor":"Demo operator","status":"active"}'
```

Edits require the current `expected_version`; stale versions return 409 and do
not append. Updateable fields are title, summary, course, supervisor, team, tags
and status. Valid lifecycle transitions are planned → active → completed and
completed → active. Use the separate archive route with `expected_version`,
`actor` and `reason`. Archived projects remain readable and cannot be edited.

Domain failures use `{ "error": "message" }`: 400 invalid data, 404 missing
project, 409 duplicate ID/version conflict, 403 writes to read-only replica, 500
integrity/storage failure. Unknown JSON fields are rejected; the maximum HTTP
request body is 128 KiB and persisted events are capped at 64 KiB.

`POST /api/audit` returns `signed_head_verified`, `verified_blocks`,
`missing_blocks`, `verified_bytes`, `length` and `public_key`. Empty logs have no
signed head. A successful health response or previously displayed audit does not
prove that disk bytes have not changed since that check.

The UI is embedded in the compiled Rust binary. No CDN, font server, JS backend,
external database service, map API or internet connection is needed to run it
after the initial Rust build. It is not a service-worker offline web app: the local
Rust server must be running; network failures retain only the current tab's
already loaded view and disable mutations until reconnection.
