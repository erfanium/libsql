//! Backs up a real sqld into an in-memory S3, and checks the backed up database against it.
//!
//! Needs a built sqld: `cargo build -p libsql-server --bin sqld`, or `SQLD_BIN=<path>`. The test
//! is skipped without it.

mod common;

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::fake_s3::FakeS3;
use libsql_backup::backup::{Backup, BackupOptions, Outcome, SnapshotReason};
use libsql_backup::config::S3Config;
use libsql_backup::fleet::{Fleet, FleetOptions};
use libsql_backup::serve::{self, ServeOptions};
use libsql_backup::status;
use libsql_backup::store::{Conflict, Store};
use libsql_backup::sync::SyncClient;
use serde_json::{json, Value};

const ADMIN_KEY: &str = "admin-secret";

fn sqld_bin() -> Option<PathBuf> {
    let path = std::env::var_os("SQLD_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/sqld"));
    path.exists().then_some(path)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Sqld {
    child: Child,
    url: String,
    http: reqwest::Client,
    _dir: tempfile::TempDir,
}

impl Drop for Sqld {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Sqld {
    async fn start(bin: &PathBuf) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let port = free_port();
        let child = Command::new(bin)
            .arg("--db-path")
            .arg(dir.path().join("data.sqld"))
            .arg("--http-listen-addr")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--admin-key")
            .arg(ADMIN_KEY)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let sqld = Self {
            child,
            url: format!("http://127.0.0.1:{port}"),
            http: reqwest::Client::new(),
            _dir: dir,
        };
        for _ in 0..200 {
            if sqld
                .http
                .get(format!("{}/health", sqld.url))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return sqld;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("sqld did not start");
    }

    async fn create_namespace(&self, ns: &str) {
        let resp = self
            .http
            .post(format!("{}/v1/namespaces/{ns}/create", self.url))
            .bearer_auth(ADMIN_KEY)
            .json(&json!({}))
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());
    }

    /// Runs the statements, returns the first column of the first row of the last one.
    async fn exec(&self, ns: &str, sqls: &[&str]) -> Option<String> {
        let mut requests: Vec<Value> = sqls
            .iter()
            .map(|sql| json!({"type": "execute", "stmt": {"sql": sql}}))
            .collect();
        requests.push(json!({"type": "close"}));
        let resp: Value = self
            .http
            .post(format!("{}/v2/pipeline", self.url))
            .header("x-namespace", ns)
            .json(&json!({ "requests": requests }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let results = resp["results"].as_array().unwrap();
        for r in results {
            assert_eq!(r["type"], "ok", "{resp}");
        }
        let last = &results[sqls.len() - 1]["response"]["result"]["rows"];
        last.get(0).map(|row| match &row[0]["value"] {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
    }

    /// What the database looks like, to compare with the backed up copy.
    async fn fingerprint(&self, ns: &str) -> String {
        self.exec(
            ns,
            &["SELECT count(*) || ':' || coalesce(sum(length(v)), 0) || ':' || coalesce(sum(id), 0) FROM t"],
        )
        .await
        .unwrap()
    }
}

fn fingerprint_of(path: &std::path::Path) -> String {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row(
        "SELECT count(*) || ':' || coalesce(sum(length(v)), 0) || ':' || coalesce(sum(id), 0) FROM t",
        [],
        |row| row.get(0),
    )
    .unwrap()
}

async fn store(endpoint: &str) -> Store {
    Store::new(&S3Config {
        bucket: "backups".into(),
        prefix: "prod".into(),
        endpoint: Some(endpoint.into()),
        region: "us-east-1".into(),
        access_key_id: Some("key".into()),
        secret_access_key: Some("secret".into()),
        session_token: None,
    })
    .await
    .unwrap()
}

async fn check_backup(store: &Store, sqld: &Sqld, ns: &str) {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("rebuilt.db");
    common::rebuild::rebuild(store, ns, &out).await;
    assert_eq!(fingerprint_of(&out), sqld.fingerprint(ns).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn backup_rounds() {
    let _ = tracing_subscriber::fmt::try_init();
    let Some(bin) = sqld_bin() else {
        eprintln!("skipped: no sqld binary, build it or set SQLD_BIN");
        return;
    };
    let s3 = FakeS3::default();
    let endpoint = s3.start().await;
    let store = store(&endpoint).await;
    let sqld = Sqld::start(&bin).await;
    let sync = SyncClient::new(reqwest::Client::new(), &sqld.url, None);
    let backup = Backup::new(
        store.clone(),
        BackupOptions {
            max_wal_files: 3,
            max_wal_pages: 8,
            zstd_level: 3,
        },
    );

    let ns = "app";
    sqld.create_namespace(ns).await;
    sqld.exec(
        ns,
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t (v) WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 200) SELECT hex(randomblob(100)) FROM c",
        ],
    )
    .await;

    // first backup: a full snapshot
    let outcome = backup.backup(&sync, ns).await.unwrap();
    assert!(
        matches!(
            outcome,
            Outcome::Snapshot {
                reason: SnapshotReason::FirstBackup,
                ..
            }
        ),
        "{outcome:?}"
    );
    check_backup(&store, &sqld, ns).await;

    // nothing new: no object is written
    let keys = s3.keys();
    let outcome = backup.backup(&sync, ns).await.unwrap();
    assert!(matches!(outcome, Outcome::UpToDate { .. }), "{outcome:?}");
    assert_eq!(s3.keys(), keys);

    // a small write goes to a WAL segment
    sqld.exec(ns, &["INSERT INTO t (v) VALUES ('one')"]).await;
    let outcome = backup.backup(&sync, ns).await.unwrap();
    assert!(matches!(outcome, Outcome::Wal { .. }), "{outcome:?}");
    let st = status::read(&store, ns).await.unwrap().unwrap().status;
    assert_eq!(st.wal.len(), 1);
    let first_segment = st.wal[0].key.clone();
    check_backup(&store, &sqld, ns).await;

    // another small one grows that segment, and the replaced object is gone
    sqld.exec(ns, &["INSERT INTO t (v) VALUES ('two')"]).await;
    backup.backup(&sync, ns).await.unwrap();
    let st = status::read(&store, ns).await.unwrap().unwrap().status;
    assert_eq!(st.wal.len(), 1);
    assert_ne!(st.wal[0].key, first_segment);
    assert!(!s3.keys().contains(&first_segment));
    check_backup(&store, &sqld, ns).await;

    // bigger writes fill segments until a new snapshot is rolled
    let mut rolled = false;
    for _ in 0..20 {
        sqld.exec(
            ns,
            &["INSERT INTO t (v) WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 10) SELECT hex(randomblob(2000)) FROM c"],
        )
        .await;
        let outcome = backup.backup(&sync, ns).await.unwrap();
        let st = status::read(&store, ns).await.unwrap().unwrap().status;
        assert!(st.wal.len() <= 3);
        check_backup(&store, &sqld, ns).await;
        if matches!(
            outcome,
            Outcome::Snapshot {
                reason: SnapshotReason::TooManyWalFiles,
                ..
            }
        ) {
            assert!(st.wal.is_empty());
            rolled = true;
            break;
        }
    }
    assert!(rolled, "never rolled a new snapshot");

    // only the objects of the status are left
    let st = status::read(&store, ns).await.unwrap().unwrap().status;
    let mut expected: Vec<String> = st.keys().map(str::to_string).collect();
    expected.push(status::status_key(&store, ns));
    expected.sort();
    assert_eq!(s3.keys(), expected);

    // updates and deletes, not only inserts
    sqld.exec(
        ns,
        &[
            "UPDATE t SET v = 'x' WHERE id % 3 = 0",
            "DELETE FROM t WHERE id % 5 = 0",
        ],
    )
    .await;
    backup.backup(&sync, ns).await.unwrap();
    check_backup(&store, &sqld, ns).await;

    // a status written by someone else in the meantime is not overwritten
    let stale = status::read(&store, ns).await.unwrap().unwrap();
    let mut st = stale.status.clone();
    status::write(&store, ns, Some(&stale), &st).await.unwrap();
    st.last_frame_no += 1;
    let err = status::write(&store, ns, Some(&stale), &st)
        .await
        .unwrap_err();
    assert!(err.downcast_ref::<Conflict>().is_some(), "{err:#}");

    // a backup of another log (sqld recovered after a crash) starts over with a snapshot
    let current = status::read(&store, ns).await.unwrap().unwrap();
    let mut st = current.status.clone();
    st.log_id = "00000000-0000-0000-0000-000000000000".into();
    status::write(&store, ns, Some(&current), &st)
        .await
        .unwrap();
    let outcome = backup.backup(&sync, ns).await.unwrap();
    assert!(
        matches!(
            outcome,
            Outcome::Snapshot {
                reason: SnapshotReason::NewLog,
                ..
            }
        ),
        "{outcome:?}"
    );
    check_backup(&store, &sqld, ns).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_database_uses_multipart() {
    let Some(bin) = sqld_bin() else {
        eprintln!("skipped: no sqld binary, build it or set SQLD_BIN");
        return;
    };
    let s3 = FakeS3::default();
    let endpoint = s3.start().await;
    let store = store(&endpoint).await;
    let sqld = Sqld::start(&bin).await;
    let sync = SyncClient::new(reqwest::Client::new(), &sqld.url, None);
    let backup = Backup::new(store.clone(), BackupOptions::default());

    let ns = "big";
    sqld.create_namespace(ns).await;
    // random data does not compress: ~40MB, more than two upload parts
    sqld.exec(
        ns,
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)",
            "INSERT INTO t (v) WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 40) SELECT randomblob(1000000) FROM c",
        ],
    )
    .await;
    let outcome = backup.backup(&sync, ns).await.unwrap();
    let Outcome::Snapshot { uploaded_bytes, .. } = outcome else {
        panic!("{outcome:?}")
    };
    assert!(uploaded_bytes > 32 * 1024 * 1024, "{uploaded_bytes}");
    check_backup(&store, &sqld, ns).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn serve_backup_route() {
    let Some(bin) = sqld_bin() else {
        eprintln!("skipped: no sqld binary, build it or set SQLD_BIN");
        return;
    };
    let s3 = FakeS3::default();
    let endpoint = s3.start().await;
    let store = store(&endpoint).await;
    let sqld = Sqld::start(&bin).await;
    let ns = "served";
    sqld.create_namespace(ns).await;
    sqld.exec(
        ns,
        &[
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t (v) VALUES ('a')",
        ],
    )
    .await;

    let http = reqwest::Client::new();
    let port = free_port();
    let fleet = Fleet::new(
        Backup::new(store.clone(), BackupOptions::default()),
        http.clone(),
        FleetOptions {
            sqld_url: None,
            default_key: None,
            admin: None,
            concurrency: 2,
        },
    );
    tokio::spawn(serve::serve(
        fleet,
        ServeOptions {
            listen: ([127, 0, 0, 1], port).into(),
            auth_key: None,
            schedule_interval: None,
        },
    ));
    let url = format!("http://127.0.0.1:{port}");
    for _ in 0..100 {
        if http.get(format!("{url}/healthz")).send().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // no key in the body and no SQLD_DEFAULT_KEY
    let resp = http
        .post(format!("{url}/backup"))
        .json(&json!({ "namespace": ns, "source": sqld.url }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    let resp = http
        .post(format!("{url}/backup"))
        .json(&json!({ "namespace": ns, "source": sqld.url, "key": "some-key" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["namespace"], ns);
    assert_eq!(body["result"], "snapshot");

    let resp = http.get(format!("{url}/status/{ns}")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    check_backup(&store, &sqld, ns).await;
}
