use axum::extract::FromRequestParts;

use crate::connection::RequestContext;

use super::{db_factory, AppState};

#[async_trait::async_trait]
impl FromRequestParts<AppState> for RequestContext {
    type Rejection = crate::error::Error;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> std::result::Result<Self, Self::Rejection> {
        let namespace = db_factory::namespace_from_headers(
            &parts.headers,
            state.disable_default_namespace,
            state.disable_namespaces,
        )?;
        Ok(Self::new(namespace, state.namespaces.meta_store().clone()))
    }
}
