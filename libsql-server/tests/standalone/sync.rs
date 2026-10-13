//! Tests for the `/sync` endpoint, and for how a request names its namespace

use std::time::Duration;

use hyper::http::HeaderName;
use hyper::StatusCode;
use libsql::Database;
use libsql_replication::rpc::replication::sync_response::Message;
use libsql_replication::rpc::replication::{SyncResponse, SyncTrailer};
use prost::Message as _;
use serde_json::json;

use crate::common::http::Client;
use crate::common::net::TurmoilConnector;

use super::make_standalone_server;

/// The frames of a sync response, and its trailer
async fn sync(client: &Client, query: &str) -> (Vec<bytes::Bytes>, SyncTrailer, u64) {
    let resp = client
        .get(&format!("http://foo.primary:8080/sync{query}"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let estimated = resp
        .header("x-sync-estimated-frames")
        .unwrap()
        .parse()
        .unwrap();

    let mut body = resp.body_bytes().await.unwrap();
    let mut frames = Vec::new();
    let mut trailer = None;
    while !body.is_empty() {
        // nothing comes after the trailer
        assert!(trailer.is_none());
        match SyncResponse::decode_length_delimited(&mut body)
            .unwrap()
            .message
            .unwrap()
        {
            Message::Frame(frame) => frames.push(frame),
            Message::Trailer(t) => trailer = Some(t),
            Message::Error(e) => panic!("sync failed: {}", e.message),
        }
    }

    (frames, trailer.unwrap(), estimated)
}

#[test]
fn sync_frames() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let client = Client::new();
        client
            .post("http://primary:8080/v1/namespaces/foo/create", json!({}))
            .await?;

        let db =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let conn = db.connect()?;
        conn.execute("create table test (x)", ()).await?;
        conn.execute("insert into test values (12)", ()).await?;

        // a replica that holds nothing is sent the whole log
        let (frames, trailer, estimated) = sync(&client, "").await;
        assert!(!frames.is_empty());
        assert_eq!(trailer.next_offset, frames.len() as u64);
        assert_eq!(estimated, frames.len() as u64);
        assert!(!trailer.log_id.is_empty());

        // it is up to date: its next sync has no frames
        let position = format!(
            "?next_offset={}&log_id={}",
            trailer.next_offset, trailer.log_id
        );
        let (frames, same, estimated) = sync(&client, &position).await;
        assert!(frames.is_empty());
        assert_eq!(same, trailer);
        assert_eq!(estimated, 0);

        // then only what was written since
        conn.execute("insert into test values (13)", ()).await?;
        let (frames, next, _) = sync(&client, &position).await;
        assert!(!frames.is_empty());
        assert_eq!(next.next_offset, trailer.next_offset + frames.len() as u64);
        assert_eq!(next.log_id, trailer.log_id);

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn sync_refuses_a_replica_of_another_log() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let client = Client::new();
        client
            .post("http://primary:8080/v1/namespaces/foo/create", json!({}))
            .await?;

        let (_, trailer, _) = sync(&client, "").await;

        let other_log = "?next_offset=0&log_id=3e52154e-29e8-4d88-ad17-19be0b250bd2";
        let ahead = format!(
            "?next_offset={}&log_id={}",
            trailer.next_offset + 1,
            trailer.log_id
        );
        for query in [other_log, ahead.as_str()] {
            let resp = client
                .get(&format!("http://foo.primary:8080/sync{query}"))
                .await?;
            assert_eq!(resp.status(), StatusCode::CONFLICT);
            assert_eq!(resp.json_value().await?, json!({ "error": "LOG_MISMATCH" }));
        }

        let resp = client.get("http://nope.primary:8080/sync").await?;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        Ok(())
    });

    sim.run().unwrap();
}

/// The backend names the namespace of the requests it forwards in `x-namespace`: the client
/// cannot pick another one.
#[test]
fn namespace_header_wins_over_host() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", make_standalone_server);

    sim.client("test", async {
        let client = Client::new();
        for ns in ["foo", "bar"] {
            client
                .post(
                    &format!("http://primary:8080/v1/namespaces/{ns}/create"),
                    json!({}),
                )
                .await?;
        }

        let x_namespace = HeaderName::from_static("x-namespace");
        let resp = client
            .post_with_headers(
                "http://foo.primary:8080/",
                &[(x_namespace, "bar")],
                json!({ "statements": ["create table only_in_bar (x)"] }),
            )
            .await?;
        assert_eq!(resp.status(), StatusCode::OK);

        let tables = |host: &'static str| {
            let client = &client;
            async move {
                client
                    .post(
                        &format!("http://{host}:8080/"),
                        json!({ "statements": ["select name from sqlite_master"] }),
                    )
                    .await
                    .unwrap()
                    .json_value()
                    .await
                    .unwrap()
            }
        };
        assert_eq!(tables("foo.primary").await[0]["results"]["rows"], json!([]));
        assert_eq!(
            tables("bar.primary").await[0]["results"]["rows"],
            json!([["only_in_bar"]])
        );

        Ok(())
    });

    sim.run().unwrap();
}
