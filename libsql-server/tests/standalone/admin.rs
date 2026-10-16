use std::time::Duration;

use hyper::StatusCode;
use libsql_server::config::{AdminApiConfig, UserApiConfig};
use s3s::header::AUTHORIZATION;
use serde_json::json;
use tempfile::tempdir;

use crate::common::{
    http::Client,
    net::{SimServer as _, TestServer, TurmoilConnector},
};

#[test]
fn admin_auth() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", || async move {
        let tmp = tempdir().unwrap();
        let server = TestServer {
            path: tmp.path().to_owned().into(),
            user_api_config: UserApiConfig {
                ..Default::default()
            },
            admin_api_config: Some(AdminApiConfig {
                connector: TurmoilConnector,
                auth_key: "secretkey".into(),
            }),
            disable_namespaces: false,
            ..Default::default()
        };
        server.start_sim(8080).await?;
        Ok(())
    });

    sim.client("test", async {
        let client = Client::new();

        // the key the client sends by default is not the one of this server
        assert_eq!(
            client
                .post("http://primary:8080/v1/namespaces/foo/create", json!({}))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert!(client
            .post_with_headers(
                "http://primary:8080/v1/namespaces/foo/create",
                &[(AUTHORIZATION, "Bearer secretkey")],
                json!({})
            )
            .await
            .unwrap()
            .status()
            .is_success());

        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn no_admin_api_without_key() {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(1000))
        .build();

    sim.host("primary", || async move {
        let tmp = tempdir().unwrap();
        let server = TestServer {
            path: tmp.path().to_owned().into(),
            user_api_config: UserApiConfig {
                ..Default::default()
            },
            admin_api_config: None,
            disable_namespaces: false,
            ..Default::default()
        };
        server.start_sim(8080).await?;
        Ok(())
    });

    sim.client("test", async {
        let client = Client::new();

        assert_eq!(
            client
                .post("http://primary:8080/v1/namespaces/foo/create", json!({}))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        // metrics are not part of the admin API
        assert!(client
            .get("http://primary:8080/metrics")
            .await
            .unwrap()
            .status()
            .is_success());

        Ok(())
    });

    sim.run().unwrap();
}
