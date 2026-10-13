use hashbrown::HashSet;
use once_cell::sync::Lazy;

use crate::namespace::NamespaceName;

use super::Permission;

#[derive(Debug, serde::Deserialize, serde::Serialize, Default)]
pub struct Authorized {
    #[serde(rename = "ro", default)]
    pub read_only: Option<Scopes>,
    #[serde(rename = "rw", default)]
    pub read_write: Option<Scopes>,
    #[serde(rename = "roa", default)]
    pub read_only_attach: Option<Scopes>,
    #[serde(rename = "rwa", default)]
    pub read_write_attach: Option<Scopes>,
    /// DDL override allows ddl statement to be executed on shared_schema databases
    #[serde(rename = "ddl", default)]
    pub ddl_override: Option<Scopes>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Scope {
    Namespace(NamespaceName),
}

impl Authorized {
    pub fn has_right(&self, scope: Scope, perm: Permission) -> bool {
        match (perm, scope) {
            (Permission::Read, Scope::Namespace(ref name)) => self.can_read_ns(name),
            (Permission::Write, Scope::Namespace(ref name)) => self.can_write_ns(name),
            (Permission::AttachRead, Scope::Namespace(ref name)) => self.can_attach_ns(name),
        }
    }

    fn is_empty(&self) -> bool {
        self.read_write.is_none()
            && self.read_only.is_none()
            && self.read_only_attach.is_none()
            && self.read_write_attach.is_none()
    }

    pub fn ddl_permitted(&self, name: &NamespaceName) -> bool {
        match self.ddl_override {
            Some(ref scope) => scope.contains(name),
            None => false,
        }
    }

    fn can_write_ns(&self, name: &NamespaceName) -> bool {
        if let Some(ref scope) = self.read_write {
            if scope.contains(name) {
                return true;
            }
        }

        if let Some(ref scope) = self.read_write_attach {
            if scope.contains(name) {
                return true;
            }
        }

        // ddl override implies write
        if let Some(ref scope) = self.ddl_override {
            if scope.contains(name) {
                return true;
            }
        }

        false
    }

    fn can_read_ns(&self, name: &NamespaceName) -> bool {
        if self.can_write_ns(name) {
            return true;
        }

        if let Some(ref scope) = self.read_only {
            if scope.contains(name) {
                return true;
            }
        }

        if let Some(ref scope) = self.read_only_attach {
            if scope.contains(name) {
                return true;
            }
        }

        false
    }

    #[cfg(test)]
    pub fn perms_iter(&self) -> impl Iterator<Item = (Scope, Permission)> + '_ {
        macro_rules! perm_iter {
            ($field:ident, $perm:expr) => {
                self.$field
                    .as_ref()
                    .map(|s| s.iter())
                    .into_iter()
                    .flatten()
                    .zip(std::iter::repeat($perm))
            };
        }

        let ro_iter = perm_iter!(read_only, Permission::Read);
        let rw_iter = perm_iter!(read_write, Permission::Write);
        let ro_attach_iter = perm_iter!(read_only_attach, Permission::AttachRead);
        let rw_attach_iter = perm_iter!(read_write_attach, Permission::AttachRead);

        ro_iter
            .chain(rw_iter)
            .chain(ro_attach_iter)
            .chain(rw_attach_iter)
    }

    fn can_attach_ns(&self, name: &NamespaceName) -> bool {
        if let Some(ref scope) = self.read_only_attach {
            if scope.contains(name) {
                return true;
            }
        }

        if let Some(ref scope) = self.read_write_attach {
            if scope.contains(name) {
                return true;
            }
        }

        false
    }
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize, Default)]
pub struct Scopes {
    #[serde(rename = "ns", default)]
    pub namespaces: Option<HashSet<NamespaceName>>,
    #[serde(rename = "tags", default)]
    pub tags: Option<HashSet<String>>,
}

impl Scopes {
    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = Scope> + '_ {
        self.namespaces
            .as_ref()
            .map(|nss| nss.iter().cloned().map(|ns| Scope::Namespace(ns)))
            .into_iter()
            .flatten()
    }

    fn contains(&self, name: &NamespaceName) -> bool {
        static GID: Lazy<Option<String>> = Lazy::new(|| std::env::var("LIBSQL_GID").ok());
        match self.namespaces {
            Some(ref set) if set.contains(name) => return true,
            _ => (),
        };

        // the only tag supported right now is the gid tag. In the future, tags will be dynamically
        // settable per-namespace
        match GID.as_ref().zip(self.tags.as_ref()) {
            Some((gid, tags)) => tags.contains(gid),
            None => false,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn gid_claim() {
        std::env::set_var("LIBSQL_GID", "my_group");

        let auth = Authorized {
            read_only: Some(Scopes {
                namespaces: None,
                tags: Some(["my_group".to_string()].into_iter().collect()),
            }),
            ..Default::default()
        };

        assert!(auth.has_right(Scope::Namespace("ns".into()), Permission::Read));
        assert!(!auth.has_right(Scope::Namespace("ns".into()), Permission::Write));

        let auth = Authorized {
            read_only: Some(Scopes {
                namespaces: None,
                tags: Some(["other_group".to_string()].into_iter().collect()),
            }),
            ..Default::default()
        };

        assert!(!auth.has_right(Scope::Namespace("ns".into()), Permission::Read));
        assert!(!auth.has_right(Scope::Namespace("ns".into()), Permission::Write));
    }
}
