//! OpenAPI 3.0 documentation for the sqld HTTP surface.
//!
//! The spec is served at `/openapi.json` (no auth — the sqld ports are
//! private in the cluster) and browsed through the Swagger UI served at
//! `/docs` (assets loaded from a CDN).

use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;

use crate::http::user::AppState;

/// The hand-maintained OpenAPI 3.0 spec. Keep it in sync with the routes in
/// [`crate::http::admin::admin_routes`] and
/// [`crate::http::admin::api::platform_routes`]; the unit test below guards
/// against drift.
pub const OPENAPI_JSON: &str = include_str!("openapi.json");

const DOCS_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>sqld HTTP API</title>
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

async fn handle_openapi_json() -> Response {
    ([("content-type", "application/json")], OPENAPI_JSON).into_response()
}

async fn handle_docs() -> Html<&'static str> {
    Html(DOCS_HTML)
}

/// Build the documentation router, merged into the main user router.
pub fn docs_routes() -> Router<AppState> {
    Router::new()
        .route("/openapi.json", get(handle_openapi_json))
        .route("/docs", get(handle_docs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_parses_and_has_expected_paths() {
        let spec: serde_json::Value = serde_json::from_str(OPENAPI_JSON)
            .expect("openapi.json must be valid JSON");
        assert_eq!(spec["openapi"], "3.0.3");

        let paths = spec["paths"].as_object().expect("paths must be an object");
        for expected in [
            "/v1/namespaces/{namespace}/create",
            "/v1/namespaces/{namespace}/config",
            "/v1/namespaces/{namespace}/checkpoint",
            "/v1/namespaces/{namespace}/compact",
            "/v1/namespaces/{namespace}/replication",
            "/v1/namespaces/{from}/fork/{to}",
            "/v1/namespaces/{namespace}",
            "/v1/namespaces/{namespace}/stats",
            "/v1/namespaces/{namespace}/stats/{stats_type}",
            "/v1/diagnostics",
            "/metrics",
            "/api/queries",
            "/health",
            "/version",
            "/openapi.json",
            "/docs",
        ] {
            assert!(paths.contains_key(expected), "missing path {expected} in openapi.json");
        }

        let scheme = &spec["components"]["securitySchemes"]["AdminKey"];
        assert_eq!(scheme["type"], "http");
        assert_eq!(scheme["scheme"], "bearer");
    }
}
