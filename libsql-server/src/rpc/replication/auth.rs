use libsql_replication::rpc::replication::NAMESPACE_DOESNT_EXIST;
use tonic::Status;

use crate::namespace::{NamespaceName, NamespaceStore};

pub async fn authenticate<T>(
    namespaces: &NamespaceStore,
    namespace: NamespaceName,
    allow_user_auth_fallback: bool,
) -> Result<(), Status> {
    match namespaces.with(namespace.clone(), |_| ()).await {
        Ok(_) => {}
        Err(e) => match e.as_ref() {
            crate::error::Error::NamespaceDoesntExist(_) if allow_user_auth_fallback => {}
            crate::error::Error::NamespaceDoesntExist(_) => {
                return Err(tonic::Status::failed_precondition(NAMESPACE_DOESNT_EXIST))
            }
            _ => {
                return Err(Status::internal(format!(
                    "Error loading namespace: {}",
                    e
                )))
            }
        },
    }

    Ok(())
}
