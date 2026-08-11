use std::sync::Arc;

use axum::extract::{FromRequestParts, Path};
use hyper::http::request::Parts;
use hyper::HeaderMap;

use crate::auth::Authenticated;
use crate::connection::MakeConnection;
use crate::database::Connection;
use crate::error::Error;
use crate::namespace::NamespaceName;

use super::AppState;

/// Resolve the request's authentication and namespace.
///
/// Namespace routing happens through the `x-namespace` header; requests are
/// executed with full access. The sqld ports are private in the cluster and
/// only reachable through the backend, which is responsible for
/// authentication.
pub(crate) async fn authenticate_request(
    parts: &mut Parts,
    state: &AppState,
) -> crate::Result<(Authenticated, NamespaceName)> {
    let namespace = namespace_from_headers(
        &parts.headers,
        state.disable_default_namespace,
        state.disable_namespaces,
    )?;

    Ok((Authenticated::FullAccess, namespace))
}

pub struct MakeConnectionExtractor(pub Arc<dyn MakeConnection<Connection = Connection>>);

#[async_trait::async_trait]
impl FromRequestParts<AppState> for MakeConnectionExtractor {
    type Rejection = Error;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let auth = Authenticated::from_request_parts(parts, state).await?;
        let ns = namespace_from_headers(
            &parts.headers,
            state.disable_default_namespace,
            state.disable_namespaces,
        )?;
        Ok(Self(
            state
                .namespaces
                .with_authenticated(ns, auth, |ns| ns.db.connection_maker())
                .await?,
        ))
    }
}

pub fn namespace_from_headers(
    headers: &HeaderMap,
    disable_default_namespace: bool,
    disable_namespaces: bool,
) -> crate::Result<NamespaceName> {
    if disable_namespaces {
        return Ok(NamespaceName::default());
    }

    if let Some(ns) = headers
        .get("x-namespace")
        .and_then(|h| h.to_str().ok())
        .and_then(|ns| NamespaceName::from_string(ns.to_string()).ok())
    {
        return Ok(ns);
    }

    if !disable_default_namespace {
        Ok(NamespaceName::default())
    } else {
        Err(Error::InvalidHost("missing x-namespace header".into()))
    }
}

pub struct MakeConnectionExtractorPath(pub Arc<dyn MakeConnection<Connection = Connection>>);
#[async_trait::async_trait]
impl FromRequestParts<AppState> for MakeConnectionExtractorPath {
    type Rejection = Error;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let auth = Authenticated::from_request_parts(parts, state).await?;
        let Path((ns, _)) = Path::<(NamespaceName, String)>::from_request_parts(parts, state)
            .await
            .map_err(|e| Error::InvalidPath(e.to_string()))?;
        Ok(Self(
            state
                .namespaces
                .with_authenticated(ns, auth, |ns| ns.db.connection_maker())
                .await?,
        ))
    }
}
