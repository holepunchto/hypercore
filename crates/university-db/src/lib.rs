//! Educational single-writer project registry. Actor names are audit labels, not authentication.
//! Each edit is a new signed log block; a projection is rebuilt only from verified events.
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use shadw_core::{AuditReport, Core, CoreInfo, ReplicationBundle};
use std::{collections::BTreeMap, path::Path};

pub type Result<T> = std::result::Result<T, DbError>;
const MAX_EVENT_BYTES: usize = 65_536;

#[derive(Debug, thiserror::Error)]
pub enum DbError {
    #[error("invalid project data: {0}")]
    Validation(String),
    #[error("project not found: {0}")]
    NotFound(String),
    #[error("project already exists: {0}")]
    AlreadyExists(String),
    #[error("revision conflict: expected {expected}, current {actual}")]
    Conflict { expected: u64, actual: u64 },
    #[error("database is read-only")]
    ReadOnly,
    #[error("invalid event at sequence {sequence}: {reason}")]
    CorruptReplay { sequence: u64, reason: String },
    #[error("log operation failed: {0}")]
    Core(String),
}
fn core_error(error: impl std::fmt::Display) -> DbError {
    DbError::Core(error.to_string())
}
fn invalid(message: impl Into<String>) -> DbError {
    DbError::Validation(message.into())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatus {
    #[default]
    Planned,
    Active,
    Completed,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Project {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub course: String,
    pub supervisor: String,
    pub team: Vec<String>,
    pub tags: Vec<String>,
    pub status: ProjectStatus,
    pub version: u64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateProject {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub course: String,
    pub supervisor: String,
    pub team: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub actor: String,
    #[serde(default)]
    pub status: ProjectStatus,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateProject {
    pub expected_version: u64,
    pub actor: String,
    pub title: Option<String>,
    pub summary: Option<String>,
    pub course: Option<String>,
    pub supervisor: Option<String>,
    pub team: Option<Vec<String>>,
    pub tags: Option<Vec<String>>,
    pub status: Option<ProjectStatus>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveProject {
    pub expected_version: u64,
    pub actor: String,
    pub reason: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectQuery {
    pub search: Option<String>,
    pub status: Option<ProjectStatus>,
    pub course: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbStats {
    pub all: u64,
    pub active: u64,
    pub planned: u64,
    pub completed: u64,
    pub archived: u64,
    pub event_count: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectEvent {
    pub schema_version: u32,
    pub sequence: u64,
    pub project_id: String,
    pub occurred_at: String,
    pub actor: String,
    pub kind: ProjectEventKind,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectEventKind {
    Create {
        project: Project,
    },
    Update {
        changes: UpdateProject,
    },
    Archive {
        expected_version: u64,
        reason: String,
    },
}

pub struct UniversityDb {
    core: Core,
    projects: BTreeMap<String, Project>,
    events: Vec<ProjectEvent>,
}
impl UniversityDb {
    pub async fn create(path: &Path) -> Result<Self> {
        Self::from_core(Core::create(path).await.map_err(core_error)?).await
    }
    pub async fn open(path: &Path) -> Result<Self> {
        Self::from_core(Core::open(path).await.map_err(core_error)?).await
    }
    pub async fn from_core(mut core: Core) -> Result<Self> {
        let mut projects = BTreeMap::new();
        let mut events = Vec::new();
        for sequence in 0..core.info().length {
            let bytes = core
                .get(sequence)
                .await
                .map_err(core_error)?
                .ok_or_else(|| DbError::CorruptReplay {
                    sequence,
                    reason: "missing block; database requires complete event history".into(),
                })?;
            let parsed = (|| {
                if bytes.len() > MAX_EVENT_BYTES {
                    return Err(invalid("event exceeds 64 KiB"));
                }
                let event: ProjectEvent = serde_json::from_slice(&bytes)
                    .map_err(|_| invalid("event is not a supported JSON schema"))?;
                apply_event(&mut projects, &event, sequence)?;
                Ok(event)
            })();
            let event = parsed.map_err(|error: DbError| DbError::CorruptReplay {
                sequence,
                reason: error.to_string(),
            })?;
            events.push(event);
        }
        Ok(Self {
            core,
            projects,
            events,
        })
    }
    pub fn info(&self) -> CoreInfo {
        self.core.info()
    }
    pub async fn audit(&mut self) -> Result<AuditReport> {
        self.core.audit().await.map_err(core_error)
    }
    pub fn into_core(self) -> Core {
        self.core
    }
    pub async fn create_project(&mut self, request: CreateProject) -> Result<Project> {
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let project = Project {
            id: request.id.clone(),
            title: request.title,
            summary: request.summary,
            course: request.course,
            supervisor: request.supervisor,
            team: request.team,
            tags: request.tags,
            status: request.status,
            version: 1,
            created_at: timestamp.clone(),
            updated_at: timestamp.clone(),
        };
        self.commit(ProjectEvent {
            schema_version: 1,
            sequence: self.events.len() as u64,
            project_id: request.id,
            occurred_at: timestamp,
            actor: request.actor,
            kind: ProjectEventKind::Create { project },
        })
        .await
    }
    pub async fn update_project(&mut self, id: &str, changes: UpdateProject) -> Result<Project> {
        self.commit(ProjectEvent {
            schema_version: 1,
            sequence: self.events.len() as u64,
            project_id: id.to_owned(),
            occurred_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            actor: changes.actor.clone(),
            kind: ProjectEventKind::Update { changes },
        })
        .await
    }
    pub async fn archive_project(&mut self, id: &str, request: ArchiveProject) -> Result<Project> {
        self.commit(ProjectEvent {
            schema_version: 1,
            sequence: self.events.len() as u64,
            project_id: id.to_owned(),
            occurred_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            actor: request.actor,
            kind: ProjectEventKind::Archive {
                expected_version: request.expected_version,
                reason: request.reason,
            },
        })
        .await
    }
    async fn commit(&mut self, event: ProjectEvent) -> Result<Project> {
        if !self.core.info().writable {
            return Err(DbError::ReadOnly);
        }
        // Validate an isolated projection before changing the authenticated log.
        let mut next = self.projects.clone();
        let project = apply_event(&mut next, &event, self.events.len() as u64)?;
        let bytes =
            serde_json::to_vec(&event).map_err(|_| invalid("event serialization failed"))?;
        if bytes.len() > MAX_EVENT_BYTES {
            return Err(invalid("event exceeds 64 KiB"));
        }
        self.core.append(&bytes).await.map_err(core_error)?;
        self.projects = next;
        self.events.push(event);
        Ok(project)
    }
    /// Deterministic ID ordering, case-insensitive substring search and exact course filtering.
    pub fn list_projects(&self, query: ProjectQuery) -> Vec<Project> {
        let needle = query.search.unwrap_or_default().trim().to_lowercase();
        self.projects
            .values()
            .filter(|p| {
                if query.status.is_some_and(|status| p.status != status) {
                    return false;
                }
                if query
                    .course
                    .as_ref()
                    .is_some_and(|course| !p.course.eq_ignore_ascii_case(course.trim()))
                {
                    return false;
                }
                if needle.is_empty() {
                    return true;
                }
                [&p.id, &p.title, &p.summary, &p.course, &p.supervisor]
                    .into_iter()
                    .chain(p.team.iter())
                    .chain(p.tags.iter())
                    .any(|text| text.to_lowercase().contains(&needle))
            })
            .cloned()
            .collect()
    }
    pub fn get_project(&self, id: &str) -> Option<Project> {
        self.projects.get(id).cloned()
    }
    pub fn history(&self, id: &str) -> Vec<ProjectEvent> {
        self.events
            .iter()
            .filter(|event| event.project_id == id)
            .cloned()
            .collect()
    }
    pub fn stats(&self) -> DbStats {
        let mut result = DbStats {
            all: self.projects.len() as u64,
            event_count: self.events.len() as u64,
            ..DbStats::default()
        };
        for project in self.projects.values() {
            match project.status {
                ProjectStatus::Planned => result.planned += 1,
                ProjectStatus::Active => result.active += 1,
                ProjectStatus::Completed => result.completed += 1,
                ProjectStatus::Archived => result.archived += 1,
            }
        }
        result
    }
    pub async fn export_bundle(&mut self) -> Result<ReplicationBundle> {
        let indices: Vec<u64> = (0..self.core.info().length).collect();
        self.core.export_bundle(&indices).await.map_err(core_error)
    }
}

fn bounded(label: &str, value: &str, max: usize, empty: bool) -> Result<()> {
    if value.len() > max
        || (!empty && value.trim().is_empty())
        || value.chars().any(|ch| ch.is_control())
    {
        return Err(invalid(format!(
            "{label} must be {}1..={max} UTF-8 bytes without control characters",
            if empty { "0 or " } else { "" }
        )));
    }
    Ok(())
}
fn validate_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(invalid(
            "id must contain 1..=64 ASCII letters, digits, hyphens or underscores",
        ));
    }
    Ok(())
}
fn validate_project(project: &Project) -> Result<()> {
    validate_id(&project.id)?;
    bounded("title", &project.title, 200, false)?;
    bounded("summary", &project.summary, 8000, true)?;
    bounded("course", &project.course, 120, false)?;
    bounded("supervisor", &project.supervisor, 160, false)?;
    if project.team.is_empty() || project.team.len() > 30 || project.tags.len() > 20 {
        return Err(invalid("team must have 1..=30 members; tags at most 20"));
    }
    for (label, list, max) in [
        ("team member", &project.team, 160),
        ("tag", &project.tags, 64),
    ] {
        let mut seen = std::collections::BTreeSet::new();
        for item in list {
            bounded(label, item, max, false)?;
            if !seen.insert(item.trim().to_lowercase()) {
                return Err(invalid(format!("duplicate {label}")));
            }
        }
    }
    Ok(())
}
fn revision(project: &Project, expected: u64) -> Result<()> {
    if expected != project.version {
        return Err(DbError::Conflict {
            expected,
            actual: project.version,
        });
    }
    if project.status == ProjectStatus::Archived {
        return Err(invalid("archived projects are immutable"));
    }
    Ok(())
}
fn apply_event(
    projects: &mut BTreeMap<String, Project>,
    event: &ProjectEvent,
    sequence: u64,
) -> Result<Project> {
    if event.schema_version != 1 || event.sequence != sequence {
        return Err(invalid("unsupported schema or non-contiguous sequence"));
    }
    validate_id(&event.project_id)?;
    bounded("actor", &event.actor, 160, false)?;
    if event.occurred_at.len() > 40 || DateTime::parse_from_rfc3339(&event.occurred_at).is_err() {
        return Err(invalid("invalid RFC3339 event timestamp"));
    }
    let mut project = match &event.kind {
        ProjectEventKind::Create { project } => {
            if projects.contains_key(&event.project_id) {
                return Err(DbError::AlreadyExists(event.project_id.clone()));
            }
            if project.id != event.project_id
                || project.version != 1
                || project.status != ProjectStatus::Planned
                || project.created_at != event.occurred_at
                || project.updated_at != event.occurred_at
            {
                return Err(invalid(
                    "new projects must start at planned revision 1 with matching identity and timestamps",
                ));
            }
            project.clone()
        }
        ProjectEventKind::Update { changes } => {
            let mut project = projects
                .get(&event.project_id)
                .cloned()
                .ok_or_else(|| DbError::NotFound(event.project_id.clone()))?;
            revision(&project, changes.expected_version)?;
            if changes.actor != event.actor {
                return Err(invalid("event actor mismatch"));
            }
            if changes.title.is_none()
                && changes.summary.is_none()
                && changes.course.is_none()
                && changes.supervisor.is_none()
                && changes.team.is_none()
                && changes.tags.is_none()
                && changes.status.is_none()
            {
                return Err(invalid("update must change at least one field"));
            }
            if let Some(status) = changes.status {
                let allowed = status == project.status
                    || matches!(
                        (project.status, status),
                        (ProjectStatus::Planned, ProjectStatus::Active)
                            | (ProjectStatus::Active, ProjectStatus::Completed)
                            | (ProjectStatus::Completed, ProjectStatus::Active)
                    );
                if !allowed {
                    return Err(invalid(
                        "invalid status transition; use the archive command to archive",
                    ));
                }
                project.status = status;
            }
            if let Some(value) = &changes.title {
                project.title.clone_from(value);
            }
            if let Some(value) = &changes.summary {
                project.summary.clone_from(value);
            }
            if let Some(value) = &changes.course {
                project.course.clone_from(value);
            }
            if let Some(value) = &changes.supervisor {
                project.supervisor.clone_from(value);
            }
            if let Some(value) = &changes.team {
                project.team.clone_from(value);
            }
            if let Some(value) = &changes.tags {
                project.tags.clone_from(value);
            }
            project.version = project
                .version
                .checked_add(1)
                .ok_or_else(|| invalid("revision overflow"))?;
            project
        }
        ProjectEventKind::Archive {
            expected_version,
            reason,
        } => {
            bounded("archive reason", reason, 2000, false)?;
            let mut project = projects
                .get(&event.project_id)
                .cloned()
                .ok_or_else(|| DbError::NotFound(event.project_id.clone()))?;
            revision(&project, *expected_version)?;
            project.status = ProjectStatus::Archived;
            project.version = project
                .version
                .checked_add(1)
                .ok_or_else(|| invalid("revision overflow"))?;
            project
        }
    };
    validate_project(&project)?;
    project.updated_at.clone_from(&event.occurred_at);
    projects.insert(event.project_id.clone(), project.clone());
    Ok(project)
}
