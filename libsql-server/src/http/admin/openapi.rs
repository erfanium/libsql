//! The description of the admin API, generated from its handlers: what is served at
//! `/openapi.json`, and browsed at `/docs`.

use axum::response::Html;
use axum::routing::get;
use axum::Json;
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::{Modify, OpenApi};

use crate::replication::ReplicationInfo;

use super::{stats, CreateNamespaceReq, HttpDatabaseConfig};

/// Every handler of the admin router is listed here: one that is not does not show in the docs.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "sqld admin API",
        description = "Management of the namespaces of a sqld server. Every request carries the admin key as a bearer token."
    ),
    paths(
        super::handle_create_namespace,
        super::handle_get_config,
        super::handle_post_config,
        super::handle_fork_namespace,
        super::handle_delete_namespace,
        super::handle_checkpoint,
        super::handle_compact,
        super::handle_replication,
        stats::handle_stats,
        super::handle_diagnostics,
        super::handle_metrics,
        super::handle_set_log_filter,
        super::enable_profile_heap,
        super::disable_profile_heap,
        super::delete_profile_heap,
    ),
    components(schemas(CreateNamespaceReq, HttpDatabaseConfig, ReplicationInfo)),
    modifiers(&AdminKey)
)]
struct ApiDoc;

struct AdminKey;

impl Modify for AdminKey {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let scheme = SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer));
        openapi
            .components
            .get_or_insert_with(Default::default)
            .add_security_scheme("admin_key", scheme);
    }
}

// The assets of Swagger UI are loaded by the browser, from a CDN.
const DOCS_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>sqld admin API</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
  <style>body { margin: 0; }</style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      url: "/openapi.json",
      dom_id: "#swagger-ui",
      persistAuthorization: true,
      deepLinking: true,
    });
  </script>
</body>
</html>"##;

async fn handle_openapi() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

async fn handle_docs() -> Html<&'static str> {
    Html(DOCS_HTML)
}

pub(super) fn router() -> axum::Router {
    axum::Router::new()
        .route("/openapi.json", get(handle_openapi))
        .route("/docs", get(handle_docs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_describes_the_admin_routes() {
        let spec = serde_json::to_value(ApiDoc::openapi()).unwrap();

        let paths = spec["paths"].as_object().unwrap();
        for path in [
            "/v1/namespaces/{namespace}/create",
            "/v1/namespaces/{namespace}/config",
            "/v1/namespaces/{namespace}/fork/{to}",
            "/v1/namespaces/{namespace}",
            "/v1/namespaces/{namespace}/checkpoint",
            "/v1/namespaces/{namespace}/compact",
            "/v1/namespaces/{namespace}/replication",
            "/v1/namespaces/{namespace}/stats",
            "/v1/diagnostics",
            "/metrics",
            "/log-filter",
        ] {
            assert!(paths.contains_key(path), "{path} is not documented");
        }

        let config = &paths["/v1/namespaces/{namespace}/config"];
        assert!(config["get"].is_object() && config["post"].is_object());
        assert_eq!(
            config["get"]["security"][0]["admin_key"],
            serde_json::json!([])
        );

        let scheme = &spec["components"]["securitySchemes"]["admin_key"];
        assert_eq!(scheme["type"], "http");
        assert_eq!(scheme["scheme"], "bearer");
    }
}
