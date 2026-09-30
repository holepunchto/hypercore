# university-db

A native Rust educational, event-sourced university-project registry. The only
persistence authority is a SHADW Core authenticated log; there is no Node runtime
or SQLite dependency. All example people and projects are synthetic.

- Creates start **planned**, revision 1.
- Updates support planned → active → completed, and completed → active to reopen.
- Archiving uses a separate command with a reason; archived projects are immutable.
- Edits and archives append events. They never truncate or erase prior history.
- `expected_version` rejects stale edits before any log mutation.
- An actor is a recorded label, **not** authenticated identity or authorization.
- An authenticated event can still be invalid domain data: reopening verifies
  every block and then rejects invalid schema, sequence, identity or transitions.
- The database requires complete event history. Sparse log replicas are useful at
  the core layer, but cannot present an incomplete materialized registry as complete.

`UniversityDb::{create,open,from_core}` rebuild the registry from verified event
blocks. `list_projects` returns ID-sorted results with optional case-insensitive
substring search, status, and exact course filters. `get_project` and `history`
return detached DTOs. `export_bundle` exports all event blocks; trust its writer
key independently before importing into a public read-only core.

`DbStats` contains `all`, `planned`, `active`, `completed`, `archived` project
counts, plus `event_count` (all revisions, not just current projects).

This is a single-writer teaching database with an in-memory search projection,
not a multi-writer distributed database, access-control service, encryption
system, or production student-record system. Events are retained, including
superseded information; do not use real student personal data in the demo.
