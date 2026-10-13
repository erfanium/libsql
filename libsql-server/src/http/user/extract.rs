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
        let (auth, namespace) = db_factory::authenticate_request(parts, state).await?;
        Ok(Self::new(
            auth,
            namespace,
            state.namespaces.meta_store().clone(),
        ))
    }
}
