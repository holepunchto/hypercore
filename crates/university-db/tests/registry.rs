use shadw_core::Core;
use university_db::*;

fn create(id: &str) -> CreateProject {
    CreateProject {
        id: id.into(),
        title: "Synthetic campus sensor study".into(),
        summary: "A fictional research project using public environmental samples.".into(),
        course: "CS401".into(),
        supervisor: "Dr. Example".into(),
        team: vec![
            "Student Alpha (synthetic)".into(),
            "Student Beta (synthetic)".into(),
        ],
        tags: vec!["sensors".into(), "rust".into()],
        actor: "Demo operator".into(),
        status: ProjectStatus::Planned,
    }
}
fn update(version: u64) -> UpdateProject {
    UpdateProject {
        expected_version: version,
        actor: "Demo operator".into(),
        status: Some(ProjectStatus::Active),
        ..UpdateProject::default()
    }
}

#[tokio::test]
async fn create_update_archive_reopen_retains_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = UniversityDb::create(&dir.path().join("writer"))
        .await
        .unwrap();
    assert_eq!(
        db.create_project(create("research-1"))
            .await
            .unwrap()
            .version,
        1
    );
    assert_eq!(
        db.update_project("research-1", update(1))
            .await
            .unwrap()
            .status,
        ProjectStatus::Active
    );
    let archived = db
        .archive_project(
            "research-1",
            ArchiveProject {
                expected_version: 2,
                actor: "Demo operator".into(),
                reason: "Semester ended".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(archived.version, 3);
    assert_eq!(db.info().length, 3);
    let history = db.history("research-1");
    let stats = db.stats();
    assert_eq!(stats.archived, 1);
    assert_eq!(stats.event_count, 3);
    db.audit().await.unwrap();
    drop(db);
    let mut reopened = UniversityDb::open(&dir.path().join("writer"))
        .await
        .unwrap();
    assert_eq!(reopened.get_project("research-1"), Some(archived));
    assert_eq!(reopened.history("research-1"), history);
    assert_eq!(reopened.stats(), stats);
    assert!(
        reopened
            .update_project("research-1", update(3))
            .await
            .is_err()
    );
    assert_eq!(reopened.info().length, 3);
}

#[tokio::test]
async fn stale_revision_and_duplicate_create_do_not_append() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = UniversityDb::create(&dir.path().join("writer"))
        .await
        .unwrap();
    db.create_project(create("p1")).await.unwrap();
    db.update_project("p1", update(1)).await.unwrap();
    assert!(matches!(
        db.update_project("p1", update(1)).await,
        Err(DbError::Conflict {
            expected: 1,
            actual: 2
        })
    ));
    assert!(matches!(
        db.create_project(create("p1")).await,
        Err(DbError::AlreadyExists(_))
    ));
    assert!(matches!(
        db.update_project("missing", update(1)).await,
        Err(DbError::NotFound(_))
    ));
    assert_eq!(db.info().length, 2);
    assert_eq!(db.get_project("p1").unwrap().version, 2);
}

#[tokio::test]
async fn queries_are_searchable_filtered_and_deterministic() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = UniversityDb::create(&dir.path().join("writer"))
        .await
        .unwrap();
    db.create_project(create("zulu")).await.unwrap();
    let mut second = create("alpha");
    second.course = "BIO202".into();
    second.tags = vec!["Botany".into()];
    db.create_project(second).await.unwrap();
    db.update_project("alpha", update(1)).await.unwrap();
    assert_eq!(
        db.list_projects(ProjectQuery::default())
            .iter()
            .map(|p| p.id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "zulu"]
    );
    assert_eq!(
        db.list_projects(ProjectQuery {
            search: Some("BOTANY".into()),
            ..ProjectQuery::default()
        })[0]
            .id,
        "alpha"
    );
    assert_eq!(
        db.list_projects(ProjectQuery {
            course: Some("cs401".into()),
            status: Some(ProjectStatus::Planned),
            search: None
        })[0]
            .id,
        "zulu"
    );
    assert!(
        db.list_projects(ProjectQuery {
            search: Some("not found".into()),
            ..ProjectQuery::default()
        })
        .is_empty()
    );
    assert_eq!(
        db.list_projects(ProjectQuery {
            search: Some("Student Alpha".into()),
            ..ProjectQuery::default()
        })
        .len(),
        2
    );
    assert_eq!(db.stats().active, 1);
}

#[tokio::test]
async fn invalid_fields_do_not_change_projection_or_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = UniversityDb::create(&dir.path().join("writer"))
        .await
        .unwrap();
    for request in [
        {
            let mut p = create("bad/id");
            p.title = "".into();
            p
        },
        {
            let mut p = create("p1");
            p.title = "x".repeat(201);
            p
        },
        {
            let mut p = create("p1");
            p.team = vec![];
            p
        },
        {
            let mut p = create("p1");
            p.team = vec!["Student".into(), " student ".into()];
            p
        },
        {
            let mut p = create("p1");
            p.actor = "\n".into();
            p
        },
        {
            let mut p = create("p1");
            p.status = ProjectStatus::Archived;
            p
        },
        {
            let mut p = create("p1");
            p.tags = vec!["x".repeat(65)];
            p
        },
    ] {
        assert!(matches!(
            db.create_project(request).await,
            Err(DbError::Validation(_))
        ));
    }
    assert_eq!(db.info().length, 0);
    assert_eq!(db.stats().all, 0);
    db.create_project(create("valid")).await.unwrap();
    let before = db.get_project("valid");
    let changes = UpdateProject {
        expected_version: 1,
        actor: "Demo".into(),
        title: Some("".into()),
        ..UpdateProject::default()
    };
    assert!(db.update_project("valid", changes).await.is_err());
    assert_eq!(db.get_project("valid"), before);
    assert_eq!(db.info().length, 1);
}

#[tokio::test]
async fn status_transitions_and_archiving_are_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = UniversityDb::create(&dir.path().join("writer"))
        .await
        .unwrap();
    db.create_project(create("p1")).await.unwrap();
    let mut change = update(1);
    change.status = Some(ProjectStatus::Completed);
    assert!(db.update_project("p1", change).await.is_err());
    let mut change = update(1);
    change.status = Some(ProjectStatus::Archived);
    assert!(db.update_project("p1", change).await.is_err());
    db.update_project("p1", update(1)).await.unwrap();
    let mut change = update(2);
    change.status = Some(ProjectStatus::Completed);
    assert_eq!(
        db.update_project("p1", change).await.unwrap().status,
        ProjectStatus::Completed
    );
    assert_eq!(db.stats().completed, 1);
    db.update_project("p1", update(3)).await.unwrap();
    assert!(
        db.archive_project(
            "p1",
            ArchiveProject {
                expected_version: 4,
                actor: "Demo".into(),
                reason: " ".into()
            }
        )
        .await
        .is_err()
    );
    assert_eq!(db.info().length, 4);
}

#[tokio::test]
async fn empty_update_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = UniversityDb::create(&dir.path().join("writer"))
        .await
        .unwrap();
    db.create_project(create("p1")).await.unwrap();
    assert!(
        db.update_project(
            "p1",
            UpdateProject {
                expected_version: 1,
                actor: "Demo".into(),
                ..UpdateProject::default()
            }
        )
        .await
        .is_err()
    );
    assert_eq!(db.info().length, 1);
}

#[tokio::test]
async fn complete_proof_replica_rebuilds_identical_read_only_database() {
    let writer_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let mut writer = UniversityDb::create(&writer_dir.path().join("writer"))
        .await
        .unwrap();
    writer.create_project(create("p1")).await.unwrap();
    writer.update_project("p1", update(1)).await.unwrap();
    writer.create_project(create("p2")).await.unwrap();
    let key = writer.into_core().public_key();
    let mut writer = UniversityDb::open(&writer_dir.path().join("writer"))
        .await
        .unwrap();
    let bundle = writer.export_bundle().await.unwrap();
    let mut replica = Core::create_replica(&replica_dir.path().join("replica"), key)
        .await
        .unwrap();
    replica.import_bundle(&bundle).await.unwrap();
    let mut reader = UniversityDb::from_core(replica).await.unwrap();
    assert_eq!(
        reader.list_projects(ProjectQuery::default()),
        writer.list_projects(ProjectQuery::default())
    );
    assert_eq!(reader.history("p1"), writer.history("p1"));
    assert_eq!(reader.stats(), writer.stats());
    assert!(!reader.info().writable);
    assert!(matches!(
        reader.create_project(create("no-write")).await,
        Err(DbError::ReadOnly)
    ));
    assert_eq!(reader.info().length, 3);
}

#[tokio::test]
async fn sparse_replica_cannot_masquerade_as_complete_database() {
    let writer_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let mut writer = UniversityDb::create(&writer_dir.path().join("writer"))
        .await
        .unwrap();
    writer.create_project(create("p1")).await.unwrap();
    writer.create_project(create("p2")).await.unwrap();
    let mut core = writer.into_core();
    let bundle = core.export_bundle(&[1]).await.unwrap();
    let mut replica = Core::create_replica(&replica_dir.path().join("replica"), core.public_key())
        .await
        .unwrap();
    replica.import_bundle(&bundle).await.unwrap();
    assert!(matches!(
        UniversityDb::from_core(replica).await,
        Err(DbError::CorruptReplay { sequence: 0, .. })
    ));
}

#[tokio::test]
async fn signed_but_malformed_events_are_rejected_on_replay() {
    for payload in [
        b"not json".to_vec(),
        br#"{"schema_version":999}"#.to_vec(),
        vec![b' '; 65_537],
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut core = Core::create(&dir.path().join("writer")).await.unwrap();
        core.append(&payload).await.unwrap();
        assert!(matches!(
            UniversityDb::from_core(core).await,
            Err(DbError::CorruptReplay { sequence: 0, .. })
        ));
    }
}

#[tokio::test]
async fn semantic_replay_checks_sequence_identity_and_revision() {
    for mode in ["sequence", "identity", "revision", "timestamp", "unknown"] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = UniversityDb::create(&dir.path().join("writer"))
            .await
            .unwrap();
        db.create_project(create("p1")).await.unwrap();
        let mut event = db.history("p1")[0].clone();
        event.sequence = 1;
        event.project_id = "p2".into();
        if let ProjectEventKind::Create { project } = &mut event.kind {
            project.id = "p2".into();
        }
        match mode {
            "sequence" => event.sequence = 4,
            "identity" => event.project_id = "p3".into(),
            "revision" => {
                if let ProjectEventKind::Create { project } = &mut event.kind {
                    project.version = 9;
                }
            }
            "timestamp" => event.occurred_at = "yesterday".into(),
            "unknown" => event.schema_version = 2,
            _ => unreachable!(),
        }
        let mut core = db.into_core();
        core.append(&serde_json::to_vec(&event).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            UniversityDb::from_core(core).await,
            Err(DbError::CorruptReplay { sequence: 1, .. })
        ));
    }
}

#[test]
fn unknown_request_fields_and_statuses_are_rejected() {
    assert!(
        serde_json::from_value::<UpdateProject>(
            serde_json::json!({"expected_version":1,"actor":"Demo","secret":true})
        )
        .is_err()
    );
    assert!(serde_json::from_str::<ProjectStatus>("\"deleted\"").is_err());
    assert!(serde_json::from_str::<ProjectQuery>("{\"surprise\":1}").is_err());
}

#[tokio::test]
async fn replay_rejects_signed_stale_updates_and_actor_mismatch() {
    for wrong_actor in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut db = UniversityDb::create(&dir.path().join("writer"))
            .await
            .unwrap();
        db.create_project(create("p1")).await.unwrap();
        let event = ProjectEvent {
            schema_version: 1,
            sequence: 1,
            project_id: "p1".into(),
            occurred_at: db.get_project("p1").unwrap().updated_at,
            actor: "Demo operator".into(),
            kind: ProjectEventKind::Update {
                changes: if wrong_actor {
                    let mut changes = update(1);
                    changes.actor = "Different label".into();
                    changes
                } else {
                    update(9)
                },
            },
        };
        let mut core = db.into_core();
        core.append(&serde_json::to_vec(&event).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            UniversityDb::from_core(core).await,
            Err(DbError::CorruptReplay { sequence: 1, .. })
        ));
    }
}
