//! Loopback-only educational registry. Actor labels are not authenticated accounts.
use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequest, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use clap::{Parser, Subcommand};
use serde::{Serialize, de::DeserializeOwned};
use shadw_core::{Core, ReplicationBundle};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;
use university_db::{
    ArchiveProject, CreateProject, DbError, Project, ProjectEvent, ProjectQuery, ProjectStatus,
    UniversityDb, UpdateProject,
};

#[derive(Parser)]
#[command(version, about = "Native Rust signed university-project registry")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Serve the local browser app. A new store is created only if the path is absent.
    Serve {
        #[arg(long, default_value = "data/university")]
        data: PathBuf,
        #[arg(long, default_value_t = 4192)]
        port: u16,
        /// Add clearly fictional example projects to an empty writable registry.
        #[arg(long)]
        seed_demo: bool,
    },
    /// Create a new fictional database, replicate it, verify it and reopen it.
    Demo {
        #[arg(long)]
        data: PathBuf,
    },
    /// List projects after verifying and replaying all event blocks.
    List {
        #[arg(long)]
        data: PathBuf,
    },
    /// Verify signed history and all stored event bytes.
    Verify {
        #[arg(long)]
        data: PathBuf,
    },
    /// Export signed public proofs; this never exports the signing secret.
    Export {
        #[arg(long)]
        data: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Copy verified proofs into a read-only replica under an independently pinned key.
    Replicate {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        destination: PathBuf,
        #[arg(long)]
        writer_key: String,
    },
}
type SharedDb = Arc<Mutex<UniversityDb>>;

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Serve {
            data,
            port,
            seed_demo,
        } => {
            let mut db = open_or_create(&data).await?;
            if seed_demo && db.info().length == 0 {
                seed(&mut db).await?;
            }
            let state = Arc::new(Mutex::new(db));
            let app = router(state);
            let address = SocketAddr::from(([127, 0, 0, 1], port));
            let listener = tokio::net::TcpListener::bind(address).await?;
            println!("Campus Ledger: http://{}", listener.local_addr()?);
            println!("Local educational demo; actor labels are not authentication.");
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await?;
        }
        Command::Demo { data } => {
            // Refuse reuse so rerunning a demo can never replace someone else's history.
            std::fs::create_dir(&data).context("demo requires a new directory")?;
            let writer = data.join("writer");
            let replica = data.join("replica");
            let mut db = UniversityDb::create(&writer).await?;
            seed(&mut db).await?;
            let trusted_key = db.info().public_key;
            let expected = db.list_projects(ProjectQuery::default());
            print_json(&db.audit().await?)?;
            drop(db);
            replicate(&writer, &replica, &trusted_key).await?;
            let reader = UniversityDb::open(&replica).await?;
            anyhow::ensure!(
                reader.list_projects(ProjectQuery::default()) == expected,
                "replica projection differs"
            );
            anyhow::ensure!(!reader.info().writable, "replica unexpectedly writable");
            print_json(&reader.stats())?;
            println!(
                "Verified persistence and read-only replication. Writer public key: {trusted_key}"
            );
        }
        Command::List { data } => {
            let db = UniversityDb::open(&data).await?;
            print_json(&db.list_projects(ProjectQuery::default()))?;
        }
        Command::Verify { data } => {
            let mut db = UniversityDb::open(&data).await?;
            print_json(&db.audit().await?)?;
        }
        Command::Export { data, output } => {
            let mut db = UniversityDb::open(&data).await?;
            let bundle = db.export_bundle().await?;
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(output)
                .context("export requires a new output file")?;
            serde_json::to_writer_pretty(file, &bundle)?;
        }
        Command::Replicate {
            source,
            destination,
            writer_key,
        } => {
            replicate(&source, &destination, &writer_key).await?;
        }
    }
    Ok(())
}
async fn open_or_create(path: &std::path::Path) -> Result<UniversityDb> {
    if path.exists() {
        Ok(UniversityDb::open(path).await?)
    } else {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        Ok(UniversityDb::create(path).await?)
    }
}
fn print_json(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
async fn replicate(
    source: &std::path::Path,
    destination: &std::path::Path,
    pin: &str,
) -> Result<()> {
    let key: [u8; 32] = hex::decode(pin)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("writer key must be 32 bytes of hex"))?;
    let mut writer = UniversityDb::open(source).await?;
    anyhow::ensure!(
        writer.info().public_key == hex::encode(key),
        "source does not match the pinned writer key"
    );
    let bundle = writer.export_bundle().await?;
    let mut reader = if destination.exists() {
        Core::open(destination).await?
    } else {
        Core::create_replica(destination, key).await?
    };
    anyhow::ensure!(
        reader.public_key() == key && !reader.info().writable,
        "destination must be a read-only replica of the pinned writer"
    );
    print_json(&reader.import_bundle(&bundle).await?)?;
    let mut db = UniversityDb::from_core(reader).await?;
    print_json(&db.audit().await?)?;
    Ok(())
}
async fn seed(db: &mut UniversityDb) -> Result<()> {
    if !db.info().writable || db.info().length != 0 {
        bail!("demo seed requires an empty writable registry");
    }
    let examples = [
        (
            "campus-energy",
            "Campus energy atlas",
            "Map building energy use with privacy-preserving aggregate data.",
            "ENV-402",
            "Demo supervisor Rowan",
            "sustainability",
            1,
        ),
        (
            "river-watch",
            "River watch sensors",
            "Explore water-quality observations using a fictional sensor dataset.",
            "ENG-310",
            "Demo supervisor Ellis",
            "sensors",
            1,
        ),
        (
            "open-archive",
            "Open research archive",
            "Design an accessible catalogue for student research outputs.",
            "CS-450",
            "Demo supervisor Morgan",
            "open-data",
            2,
        ),
        (
            "access-map",
            "An accessible campus",
            "Prototype a step-free route planner with synthetic accessibility data.",
            "DES-220",
            "Demo supervisor Avery",
            "accessibility",
            0,
        ),
        (
            "soil-study",
            "Soil health notebook",
            "A reproducible notebook for comparing fictional soil sample records.",
            "BIO-315",
            "Demo supervisor Casey",
            "research",
            0,
        ),
        (
            "library-pilot",
            "Library occupancy pilot",
            "Archived practice project; no real people or occupancy records.",
            "CS-201",
            "Demo supervisor Taylor",
            "prototype",
            3,
        ),
    ];
    for (id, title, summary, course, supervisor, tag, phase) in examples {
        let mut project = db
            .create_project(CreateProject {
                id: id.into(),
                title: title.into(),
                summary: summary.into(),
                course: course.into(),
                supervisor: supervisor.into(),
                team: vec!["Fictional student team".into()],
                tags: vec![tag.into(), "demo".into()],
                actor: "Demo coordinator".into(),
                status: ProjectStatus::Planned,
            })
            .await?;
        if phase == 1 || phase == 2 {
            project = db
                .update_project(
                    id,
                    UpdateProject {
                        expected_version: project.version,
                        actor: "Demo coordinator".into(),
                        status: Some(ProjectStatus::Active),
                        ..Default::default()
                    },
                )
                .await?;
        }
        if phase == 2 {
            db.update_project(
                id,
                UpdateProject {
                    expected_version: project.version,
                    actor: "Demo coordinator".into(),
                    status: Some(ProjectStatus::Completed),
                    ..Default::default()
                },
            )
            .await?;
        }
        if phase == 3 {
            db.archive_project(
                id,
                ArchiveProject {
                    expected_version: project.version,
                    actor: "Demo coordinator".into(),
                    reason: "Fictional pilot concluded; preserve history for teaching.".into(),
                },
            )
            .await?;
        }
    }
    Ok(())
}
fn router(state: SharedDb) -> Router {
    Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../web/index.html")) }),
        )
        .route(
            "/app.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                    include_str!("../web/app.js"),
                )
            }),
        )
        .route(
            "/style.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                    include_str!("../web/style.css"),
                )
            }),
        )
        .route(
            "/icon.svg",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "image/svg+xml")],
                    include_str!("../web/icon.svg"),
                )
            }),
        )
        .route("/api/health", get(health))
        .route("/api/projects", get(projects).post(create))
        .route("/api/projects/{id}", get(project).patch(update))
        .route("/api/projects/{id}/archive", post(archive))
        .route("/api/projects/{id}/history", get(history))
        .route("/api/events", get(events))
        .route("/api/audit", post(audit))
        .route("/api/export", get(export))
        .layer(DefaultBodyLimit::max(128 * 1024))
        .layer(middleware::from_fn(local_origin))
        .with_state(state)
}
// Loopback is not authentication. Reject browser cross-origin calls and DNS rebinding
// while keeping CLI/API clients without Origin usable. Never enable wildcard CORS.
async fn local_origin(headers: HeaderMap, request: axum::extract::Request, next: Next) -> Response {
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let hostname = host.split(':').next().unwrap_or("");
    let origin_ok = headers
        .get(header::ORIGIN)
        .map(|v| v.to_str().ok() == Some(format!("http://{host}").as_str()))
        .unwrap_or(true);
    if !matches!(hostname, "localhost" | "127.0.0.1") || !origin_ok {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error":"Only same-origin loopback requests are accepted"})),
        )
            .into_response();
    }
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response.headers_mut().insert(header::CONTENT_SECURITY_POLICY, "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'".parse().unwrap());
    response
}
// Keep extractor failures in the same JSON error envelope as domain failures.
struct ApiJson<T>(T);
impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = Response;
    async fn from_request(
        request: axum::extract::Request,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(request, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(|error| {
                (
                    error.status(),
                    Json(serde_json::json!({"error":error.body_text()})),
                )
                    .into_response()
            })
    }
}
struct ApiError(DbError);
impl From<DbError> for ApiError {
    fn from(error: DbError) -> Self {
        Self(error)
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self.0 {
            DbError::Validation(m) => (StatusCode::BAD_REQUEST, m),
            DbError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            DbError::AlreadyExists(m) => (StatusCode::CONFLICT, m),
            e @ DbError::Conflict { .. } => (StatusCode::CONFLICT, e.to_string()),
            DbError::ReadOnly => (StatusCode::FORBIDDEN, "This replica is read-only".into()),
            error => {
                eprintln!("Registry operation failed: {error}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Database integrity or storage operation failed; inspect the local server log"
                        .into(),
                )
            }
        };
        (status, Json(serde_json::json!({"error":message}))).into_response()
    }
}
async fn health(State(db): State<SharedDb>) -> Json<serde_json::Value> {
    let db = db.lock().await;
    Json(
        serde_json::json!({"status":"ok","runtime":"rust","mode":"university-example","info":db.info(),"stats":db.stats()}),
    )
}
async fn projects(
    State(db): State<SharedDb>,
    Query(query): Query<ProjectQuery>,
) -> Json<Vec<Project>> {
    Json(db.lock().await.list_projects(query))
}
async fn project(
    State(db): State<SharedDb>,
    Path(id): Path<String>,
) -> Result<Json<Project>, ApiError> {
    db.lock()
        .await
        .get_project(&id)
        .map(Json)
        .ok_or(ApiError(DbError::NotFound(id)))
}
async fn create(
    State(db): State<SharedDb>,
    ApiJson(input): ApiJson<CreateProject>,
) -> Result<(StatusCode, Json<Project>), ApiError> {
    Ok((
        StatusCode::CREATED,
        Json(db.lock().await.create_project(input).await?),
    ))
}
async fn update(
    State(db): State<SharedDb>,
    Path(id): Path<String>,
    ApiJson(input): ApiJson<UpdateProject>,
) -> Result<Json<Project>, ApiError> {
    Ok(Json(db.lock().await.update_project(&id, input).await?))
}
async fn archive(
    State(db): State<SharedDb>,
    Path(id): Path<String>,
    ApiJson(input): ApiJson<ArchiveProject>,
) -> Result<Json<Project>, ApiError> {
    Ok(Json(db.lock().await.archive_project(&id, input).await?))
}
async fn history(
    State(db): State<SharedDb>,
    Path(id): Path<String>,
) -> Result<Json<Vec<ProjectEvent>>, ApiError> {
    let db = db.lock().await;
    if db.get_project(&id).is_none() {
        return Err(ApiError(DbError::NotFound(id)));
    }
    Ok(Json(db.history(&id)))
}
async fn events(State(db): State<SharedDb>) -> Json<Vec<ProjectEvent>> {
    let db = db.lock().await;
    let mut events: Vec<_> = db
        .list_projects(ProjectQuery::default())
        .iter()
        .flat_map(|p| db.history(&p.id))
        .collect();
    events.sort_by_key(|e| std::cmp::Reverse(e.sequence));
    events.truncate(50);
    Json(events)
}
async fn audit(State(db): State<SharedDb>) -> Result<Json<shadw_core::AuditReport>, ApiError> {
    Ok(Json(db.lock().await.audit().await?))
}
async fn export(
    State(db): State<SharedDb>,
) -> Result<
    (
        [(header::HeaderName, &'static str); 1],
        Json<ReplicationBundle>,
    ),
    ApiError,
> {
    Ok((
        [(
            header::CONTENT_DISPOSITION,
            "attachment; filename=campus-ledger-proofs.json",
        )],
        Json(db.lock().await.export_bundle().await?),
    ))
}
